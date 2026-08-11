"""Build a pairwise Wikidata statistics matrix for WikiProject analysis."""

import json
import os
import sys
from itertools import combinations_with_replacement
from multiprocessing import get_context
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import WikidataScholarlyArticleFilter

# ---- Runtime config ----
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", "4"))
READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", "10"))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", "2000"))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/pair_filter_matrix.json")
SPARQL_ENDPOINT = os.environ.get(
    "WIKIDATA_SPARQL_ENDPOINT",
    os.environ.get("SPARQL_ENDPOINT", "https://query.wikidata.org/sparql"),
)
SPARQL_TIMEOUT_SECONDS = int(
    os.environ.get("KNOWLEDGE_TREE_QUERY_TIMEOUT_SECONDS", "120")
)
TARGET_LANGS = tuple(
    dict.fromkeys(
        lang.strip()
        for lang in os.environ.get("TARGET_LANGS", "en,de,ar,fr").split(",")
        if lang.strip()
    )
)


# ---- Knowledge-tree query ----
KNOWLEDGE_TREE_QUERY = """#defaultView:Graph
SELECT ?item ?itemLabel ?linkTo {
{ wd:Q17379835 wdt:P279* ?item } UNION { ?item wdt:P279* wd:Q17379835 }
OPTIONAL { ?item wdt:P279 ?linkTo }
SERVICE wikibase:label {bd:serviceParam wikibase:language "en,mul" }
}
"""
KNOWLEDGE_TREE_ROOT_QID = "Q17379835"


def fetch_knowledge_tree_classes(
    endpoint=SPARQL_ENDPOINT,
    query=KNOWLEDGE_TREE_QUERY,
    timeout=SPARQL_TIMEOUT_SECONDS,
):
    """Fetch the QIDs connected to the outside-main-tree class by P279."""
    response = requests.get(
        endpoint,
        params={"query": query, "format": "json"},
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": "WikidataTextEmbedding WikiProject statistics",
        },
        timeout=timeout,
    )
    response.raise_for_status()

    try:
        bindings = response.json()["results"]["bindings"]
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("The knowledge-tree SPARQL response was not valid JSON results") from error

    class_qids = set()
    for binding in bindings:
        value = binding.get("item", {}).get("value", "")
        qid = value.rsplit("/", 1)[-1]
        if qid.startswith("Q") and qid[1:].isdigit():
            class_qids.add(qid)

    if not class_qids:
        raise RuntimeError("The knowledge-tree SPARQL query returned no item QIDs")

    return frozenset(class_qids)


# ---- Filter definitions ----
FILTERS = [
    {
        "id": "basic",
        "description": (
            "Has at least one label and has a description or claim; "
            "the disambiguation condition is not applied."
        ),
    },
    {
        "id": "no_sitelink",
        "description": "Has no sitelink on any Wikimedia project.",
    },
    {
        "id": "has_sitelink",
        "description": "Has at least one sitelink on a Wikimedia project.",
    },
    {
        "id": "has_wikipedia_sitelink",
        "description": "Has at least one sitelink whose site ID ends in wiki.",
    },
    {
        "id": "no_wikipedia_sitelink",
        "description": "Has no sitelink whose site ID ends in wiki.",
    },
    {
        "id": "is_part_of_main_knowledge_tree",
        "description": (
            "Is a Q-item whose direct instance-of values do not include a class "
            "connected to Q17379835 through P279."
        ),
    },
    {
        "id": "is_not_part_of_main_knowledge_tree",
        "description": (
            "Is a Q-item whose direct instance-of values include a class "
            "connected to Q17379835 through P279."
        ),
    },
]
FILTER_IDS = [filter_definition["id"] for filter_definition in FILTERS]
FILTER_INDEX = {filter_id: index for index, filter_id in enumerate(FILTER_IDS)}
SCHOLARLY_INSTANCE_TYPES = frozenset(WikidataScholarlyArticleFilter.instance_of_qids)


# ---- Process-shared runtime state ----
MAIN_KNOWLEDGE_TREE_CLASSES = frozenset()
SHARED_STATS = None
SHARED_LOCK = None


def get_entity_kind(entity):
    """Return the report kind for a Q-item or P-property, or None otherwise."""
    entity_id = entity.get("id", "")
    if entity_id.startswith("Q"):
        return "items"
    if entity_id.startswith("P"):
        return "properties"
    return None


def get_instance_of_ids(entity):
    """Return non-deprecated direct P31 QIDs from an entity."""
    instance_of = set()
    for claim in entity.get("claims", {}).get("P31", []):
        if not isinstance(claim, dict) or claim.get("rank") == "deprecated":
            continue
        value = (
            claim.get("mainsnak", {})
            .get("datavalue", {})
            .get("value", {})
        )
        if isinstance(value, dict) and value.get("id"):
            instance_of.add(value["id"])
    return instance_of


def has_basic_filter(entity):
    """Return whether an entity has any label and any content."""
    return bool(entity.get("labels")) and (
        bool(entity.get("descriptions")) or bool(entity.get("claims"))
    )


def is_scholarly_article(entity, instance_of=None):
    """Return whether an entity is a scholarly article under the project filter."""
    if instance_of is None:
        instance_of = get_instance_of_ids(entity)

    has_scholarly_instance = bool(instance_of & SCHOLARLY_INSTANCE_TYPES)
    has_publication_type = any(
        isinstance(claim, dict) and claim.get("rank") != "deprecated"
        for claim in entity.get("claims", {}).get("P13046", [])
    )
    return has_scholarly_instance or has_publication_type


def get_filter_results(entity, entity_kind, instance_of, knowledge_tree_classes):
    """Return the requested filter results for an already eligible entity."""
    sitelink_ids = set(entity.get("sitelinks", {}) or {})
    has_wikipedia_sitelink = any(
        sitelink_id.endswith("wiki") for sitelink_id in sitelink_ids
    )
    is_not_main_tree = entity_kind == "items" and bool(
        instance_of & knowledge_tree_classes
    )

    return {
        "basic": True,
        "no_sitelink": not sitelink_ids,
        "has_sitelink": bool(sitelink_ids),
        "has_wikipedia_sitelink": has_wikipedia_sitelink,
        "no_wikipedia_sitelink": not has_wikipedia_sitelink,
        "is_part_of_main_knowledge_tree": entity_kind == "items"
        and not is_not_main_tree,
        "is_not_part_of_main_knowledge_tree": is_not_main_tree,
    }


# ---- Worker and batch handlers ----
def collect_stats(items):
    """Collect pairwise filter counts for one dump-reader batch."""
    batch_stats = {}
    for entity in items:
        if not isinstance(entity, dict):
            continue

        entity_kind = get_entity_kind(entity)
        if entity_kind is None or not has_basic_filter(entity):
            continue

        instance_of = get_instance_of_ids(entity)
        # Scholarly articles are removed before any requested category is counted.
        if entity_kind == "items" and is_scholarly_article(entity, instance_of):
            continue

        filter_results = get_filter_results(
            entity,
            entity_kind,
            instance_of,
            MAIN_KNOWLEDGE_TREE_CLASSES,
        )

        scope_filter_results = {"all_wikidata": filter_results}
        labels = entity.get("labels", {})
        for lang in TARGET_LANGS:
            if lang in labels or "mul" in labels:
                scope_filter_results[f"language:{lang}"] = filter_results

        for scope, scoped_filter_results in scope_filter_results.items():
            batch_stats[f"{scope}:total"] = batch_stats.get(f"{scope}:total", 0) + 1
            kind_key = f"{scope}:{entity_kind}"
            batch_stats[kind_key] = batch_stats.get(kind_key, 0) + 1

            passed_indices = sorted(
                FILTER_INDEX[filter_id]
                for filter_id, passed in scoped_filter_results.items()
                if passed
            )
            for left_index, right_index in combinations_with_replacement(
                passed_indices, 2
            ):
                matrix_key = f"{scope}:matrix:{left_index}:{right_index}"
                batch_stats[matrix_key] = batch_stats.get(matrix_key, 0) + 1

    with SHARED_LOCK:
        for key, value in batch_stats.items():
            SHARED_STATS[key] = int(SHARED_STATS.get(key, 0)) + int(value)


# ---- Orchestration ----
def run_stats():
    """Fetch the class set, run the dump reader, and write the report."""
    global MAIN_KNOWLEDGE_TREE_CLASSES, SHARED_STATS, SHARED_LOCK

    # Fetch once in the parent process. The dump reader uses fork, so workers
    # inherit this immutable set without making additional SPARQL requests.
    MAIN_KNOWLEDGE_TREE_CLASSES = fetch_knowledge_tree_classes()

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

    per_language = {lang: scopes[f"language:{lang}"] for lang in TARGET_LANGS}

    output = {
        "dump_path": DUMP_PATH,
        "matrix_note": (
            "Rows and columns follow filter_order. Each cell counts eligible entities "
            "passing both filters; diagonal cells are individual filter counts."
        ),
        "eligibility_note": (
            "Only Q-items and P-properties passing the basic filter are counted. "
            "Scholarly Q-items are always removed; the disambiguation condition is not applied."
        ),
        "basic_filter_note": (
            "The basic filter requires at least one label and a description or claim. "
            "Every other filter is evaluated only after this filter."
        ),
        "target_languages": list(TARGET_LANGS),
        "per_language_note": (
            "Each language includes eligible Q/P entities having that language or a mul label. "
            "Filter results are language-independent and identical to all_wikidata."
        ),
        "knowledge_tree": {
            "root_qid": KNOWLEDGE_TREE_ROOT_QID,
            "sparql_endpoint": SPARQL_ENDPOINT,
            "class_count": len(MAIN_KNOWLEDGE_TREE_CLASSES),
            "class_qids": sorted(MAIN_KNOWLEDGE_TREE_CLASSES),
        },
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
