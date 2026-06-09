import json
import os
import sys
from itertools import combinations_with_replacement
from multiprocessing import get_context
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import WikidataScholarlyArticleFilter


# ---- Runtime config ----
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", 4))
READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", 10))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", 2000))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/pair_filter_matrix.json")
TARGET_LANGS = tuple(
    dict.fromkeys(
        lang.strip()
        for lang in os.environ.get("TARGET_LANGS", "en,de,ar,fr").split(",")
        if lang.strip()
    )
)


# ---- Filter definitions ----
EXCLUDED_INSTANCE_TYPES = {
    "not_category": "Q4167836",
    "not_list": "Q13406463",
    "not_template": "Q11266439",
    "not_disambiguation": "Q4167410",
    "not_portal": "Q4663903",
    "not_module": "Q15184295",
    "not_help_page": "Q56005592",
    "not_project_page": "Q14204246",
    "not_quiz": "Q81460944",
    "not_appendix": "Q35243371",
    "not_interface": "Q35250433",
    "not_talk_page": "Q87358148",
}

SITELINK_PROJECT_SUFFIXES = {
    "has_wikibooks_sitelink": "wikibooks",
    "has_wikinews_sitelink": "wikinews",
    "has_wikiquote_sitelink": "wikiquote",
    "has_wikisource_sitelink": "wikisource",
    "has_wikiversity_sitelink": "wikiversity",
    "has_wikivoyage_sitelink": "wikivoyage",
    "has_wiktionary_sitelink": "wiktionary",
}

EXACT_SITELINK_SITES = {
    "has_wikispecies_sitelink": "specieswiki",
    "has_wikifunctions_sitelink": "wikifunctionswiki",
    "has_mediawiki_sitelink": "mediawikiwiki",
    "has_meta_wiki_sitelink": "metawiki",
    "has_wikidata_sitelink": "wikidatawiki",
    "has_wikimedia_foundation_sitelink": "foundationwiki",
    "has_wikimania_sitelink": "wikimaniawiki",
    "has_multilingual_wikisource_sitelink": "sourceswiki",
}

FILTERS = [
    {
        "id": "basic",
        "description": (
            "Has any label and has any description or claim; "
            "does not apply the disambiguation filter."
        ),
    },
    {
        "id": "has_wikipedia_sitelink",
        "description": "Has a sitelink site ID ending in wiki.",
    },
    {
        "id": "has_wikibooks_sitelink",
        "description": "Has a sitelink with a site ID ending in wikibooks.",
    },
    {
        "id": "has_wikinews_sitelink",
        "description": "Has a sitelink with a site ID ending in wikinews.",
    },
    {
        "id": "has_wikiquote_sitelink",
        "description": "Has a sitelink with a site ID ending in wikiquote.",
    },
    {
        "id": "has_wikisource_sitelink",
        "description": "Has a sitelink with a site ID ending in wikisource.",
    },
    {
        "id": "has_wikiversity_sitelink",
        "description": "Has a sitelink with a site ID ending in wikiversity.",
    },
    {
        "id": "has_commons_sitelink",
        "description": "Has a Commons sitelink.",
    },
    {
        "id": "has_wikivoyage_sitelink",
        "description": "Has a sitelink with a site ID ending in wikivoyage.",
    },
    {
        "id": "has_abstract_wikipedia_sitelink",
        "description": "Has an Abstract Wikipedia sitelink.",
    },
    {
        "id": "has_wiktionary_sitelink",
        "description": "Has a sitelink with a site ID ending in wiktionary.",
    },
    *[
        {
            "id": filter_id,
            "description": f"Has the exact Wikidata sitelink site ID {site_id}.",
        }
        for filter_id, site_id in EXACT_SITELINK_SITES.items()
    ],
    {
        "id": "has_any_sitelink",
        "description": "Has at least one sitelink.",
    },
    {
        "id": "not_scholarly_article",
        "description": (
            "Passes basic and is not a scholarly article according to "
            "WikidataScholarlyArticleFilter."
        ),
    },
    *[
        {
            "id": filter_id,
            "description": f"Does not have instance of (P31) {qid}.",
        }
        for filter_id, qid in EXCLUDED_INSTANCE_TYPES.items()
    ],
    {
        "id": "none_of_excluded_types",
        "description": (
            "Is not scholarly and has none of the excluded instance-of values; "
            "includes main items and properties."
        ),
    },
]
FILTER_IDS = [filter_definition["id"] for filter_definition in FILTERS]
FILTER_INDEX = {filter_id: index for index, filter_id in enumerate(FILTER_IDS)}
SCHOLARLY_INSTANCE_TYPES = set(WikidataScholarlyArticleFilter.instance_of_qids)


# ---- Process-shared runtime state ----
SHARED_STATS = None
SHARED_LOCK = None


# ---- Worker and batch handlers ----
def collect_stats(items):
    global SHARED_STATS, SHARED_LOCK

    batch_stats = {}
    for entity in items:
        if not isinstance(entity, dict):
            continue

        entity_id = entity.get("id", "")
        if entity_id.startswith("Q"):
            entity_kind = "items"
        elif entity_id.startswith("P"):
            entity_kind = "properties"
        else:
            continue

        sitelinks = entity.get("sitelinks", {})
        sitelink_ids = set(sitelinks)
        instanceof = {
            value.get("id")
            for claim in entity.get("claims", {}).get("P31", [])
            if claim.get("rank") != "deprecated"
            and isinstance(
                value := claim.get("mainsnak", {}).get("datavalue", {}).get("value", {}),
                dict,
            )
            and value.get("id")
        }

        not_scholarly_article = not (
            bool(instanceof & SCHOLARLY_INSTANCE_TYPES)
            or any(
                claim.get("rank") != "deprecated"
                for claim in entity.get("claims", {}).get("P13046", [])
            )
        )
        filter_results = {
            "basic": bool(entity.get("labels")) and (
                bool(entity.get("descriptions")) or bool(entity.get("claims"))
            ),
            "has_wikipedia_sitelink": any(
                sitelink_id.endswith("wiki")
                for sitelink_id in sitelink_ids
            ),
            "has_commons_sitelink": "commonswiki" in sitelink_ids,
            "has_abstract_wikipedia_sitelink": "abstractwiki" in sitelink_ids,
            "has_any_sitelink": bool(sitelinks),
            "not_scholarly_article": not_scholarly_article,
        }

        for filter_id, suffix in SITELINK_PROJECT_SUFFIXES.items():
            filter_results[filter_id] = any(
                sitelink_id.endswith(suffix)
                for sitelink_id in sitelink_ids
            )

        for filter_id, site_id in EXACT_SITELINK_SITES.items():
            filter_results[filter_id] = site_id in sitelink_ids

        for filter_id, qid in EXCLUDED_INSTANCE_TYPES.items():
            filter_results[filter_id] = qid not in instanceof
        filter_results["none_of_excluded_types"] = not_scholarly_article and all(
            qid not in instanceof
            for qid in EXCLUDED_INSTANCE_TYPES.values()
        )
        basic_pass = filter_results["basic"]
        for filter_id in FILTER_IDS:
            if filter_id != "basic":
                filter_results[filter_id] = basic_pass and filter_results[filter_id]

        scope_filter_results = {
            "all_wikidata": filter_results,
        }
        labels = entity.get("labels", {})
        for lang in TARGET_LANGS:
            if lang in labels or "mul" in labels:
                scope_filter_results[f"language:{lang}"] = filter_results

        for scope, filter_results in scope_filter_results.items():
            batch_stats[f"{scope}:total"] = batch_stats.get(f"{scope}:total", 0) + 1
            kind_key = f"{scope}:{entity_kind}"
            batch_stats[kind_key] = batch_stats.get(kind_key, 0) + 1

            passed_indices = sorted(
                FILTER_INDEX[filter_id]
                for filter_id, passed in filter_results.items()
                if passed
            )
            for left_index, right_index in combinations_with_replacement(passed_indices, 2):
                matrix_key = f"{scope}:matrix:{left_index}:{right_index}"
                batch_stats[matrix_key] = batch_stats.get(matrix_key, 0) + 1

    with SHARED_LOCK:
        for key, value in batch_stats.items():
            SHARED_STATS[key] = int(SHARED_STATS.get(key, 0)) + int(value)


# ---- Orchestration ----
def run_stats():
    global SHARED_STATS, SHARED_LOCK

    ctx = get_context("fork")
    manager = ctx.Manager()
    SHARED_LOCK = manager.Lock()
    SHARED_STATS = manager.dict()

    reader = WikidataDumpReader(
        DUMP_PATH,
        num_processes=NUM_PROCESSES,
        queue_size=READER_QUEUE_SIZE,
        batch_size=READER_BATCH_SIZE,
    )
    reader.run(
        collect_stats,
        handler_receives_batch=True,
    )

    merged = {key: int(value) for key, value in SHARED_STATS.items()}
    manager.shutdown()

    scopes = {}
    for scope in ("all_wikidata", *(f"language:{lang}" for lang in TARGET_LANGS)):
        matrix = [[0] * len(FILTER_IDS) for _ in FILTER_IDS]
        for left_index in range(len(FILTER_IDS)):
            for right_index in range(left_index, len(FILTER_IDS)):
                value = int(merged.get(f"{scope}:matrix:{left_index}:{right_index}", 0))
                matrix[left_index][right_index] = value
                matrix[right_index][left_index] = value

        scopes[scope] = {
            "total": int(merged.get(f"{scope}:total", 0)),
            "items": int(merged.get(f"{scope}:items", 0)),
            "properties": int(merged.get(f"{scope}:properties", 0)),
            "filter_counts": {
                filter_id: matrix[index][index]
                for index, filter_id in enumerate(FILTER_IDS)
            },
            "pair_matrix": matrix,
        }

    per_language = {
        lang: scopes[f"language:{lang}"]
        for lang in TARGET_LANGS
    }

    output = {
        "dump_path": DUMP_PATH,
        "matrix_note": (
            "Rows and columns follow filter_order. Each cell counts entities passing both "
            "filters; diagonal cells are individual filter counts."
        ),
        "basic_filter_note": (
            "Every non-basic filter includes the basic filter. Pairwise cells therefore count "
            "basic plus both selected filters."
        ),
        "target_languages": list(TARGET_LANGS),
        "per_language_note": (
            "Each language includes Q/P entities having that language or a mul label. "
            "Filter results are language-independent and identical to all_wikidata."
        ),
        "filter_order": FILTER_IDS,
        "filters": FILTERS,
        "all_wikidata": scopes["all_wikidata"],
        "per_language": per_language,
        "reader": {
            "entities_processed": int(reader.iterations.value),
            "handler_errors": int(reader.handler_errors.value),
            "num_processes": NUM_PROCESSES,
            "batch_size": READER_BATCH_SIZE,
            "queue_size": READER_QUEUE_SIZE,
        },
    }

    output_dir = os.path.dirname(OUTPUT_PATH)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as output_file:
        json.dump(output, output_file, indent=2, ensure_ascii=False)

    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    run_stats()
