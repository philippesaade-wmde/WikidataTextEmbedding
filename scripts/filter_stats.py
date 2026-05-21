import json
import os
import sys
from pathlib import Path
from multiprocessing import get_context

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import WikidataItemFilter, WikidataScholarlyArticleFilter


# ---- Runtime config ----
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", 4))
READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", 128))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", 16))
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "data/filter_stats.json")


# ---- Process-local runtime state ----
ITEM_FILTERS = None
SCHOLARLY_FILTERS = None
SHARED_STATS = None
SHARED_LOCK = None


# ---- Worker and batch handlers ----
def init_worker():
    global ITEM_FILTERS, SCHOLARLY_FILTERS

    ITEM_FILTERS = {}
    SCHOLARLY_FILTERS = {}


def collect_stats(items):
    global ITEM_FILTERS, SCHOLARLY_FILTERS, SHARED_STATS, SHARED_LOCK

    batch_stats = {
        "total_entities": 0,
        "total_properties": 0,
        "all:item": 0,
        "all:scholarly": 0,
        "all:common": 0,
    }

    for entity in items:
        if not isinstance(entity, dict):
            continue

        entity_id = entity.get("id", "")
        if entity_id.startswith("Q"):
            batch_stats["total_entities"] += 1
        elif entity_id.startswith("P"):
            batch_stats["total_properties"] += 1
            continue
        else:
            continue

        langs = set(entity.get("labels", {}).keys())
        langs.update(entity.get("descriptions", {}).keys())
        if not langs:
            continue

        any_item = False
        any_scholarly = False

        for lang in langs:
            if lang not in ITEM_FILTERS:
                ITEM_FILTERS[lang] = WikidataItemFilter(lang=lang, fallback_lang=lang)
                SCHOLARLY_FILTERS[lang] = WikidataScholarlyArticleFilter(lang=lang, fallback_lang=lang)

            item_ok = ITEM_FILTERS[lang].filter(entity)
            scholarly_ok = SCHOLARLY_FILTERS[lang].filter(entity)

            if item_ok:
                key = f"{lang}:item"
                batch_stats[key] = batch_stats.get(key, 0) + 1
            if scholarly_ok:
                key = f"{lang}:scholarly"
                batch_stats[key] = batch_stats.get(key, 0) + 1
            if item_ok and scholarly_ok:
                key = f"{lang}:common"
                batch_stats[key] = batch_stats.get(key, 0) + 1

            any_item = any_item or item_ok
            any_scholarly = any_scholarly or scholarly_ok

        if any_item:
            batch_stats["all:item"] += 1
        if any_scholarly:
            batch_stats["all:scholarly"] += 1
        if any_item and any_scholarly:
            batch_stats["all:common"] += 1

    with SHARED_LOCK:
        for key, value in batch_stats.items():
            if value:
                SHARED_STATS[key] = int(SHARED_STATS.get(key, 0)) + int(value)


# ---- Orchestration ----
def run_stats():
    global SHARED_STATS, SHARED_LOCK
    ctx = get_context("fork")
    manager = ctx.Manager()
    SHARED_LOCK = manager.Lock()
    SHARED_STATS = manager.dict({
        "total_entities": 0,
        "total_properties": 0,
        "all:item": 0,
        "all:scholarly": 0,
        "all:common": 0,
    })

    reader = WikidataDumpReader(
        DUMP_PATH,
        num_processes=NUM_PROCESSES,
        queue_size=READER_QUEUE_SIZE,
        batch_size=READER_BATCH_SIZE,
    )
    reader.run(
        collect_stats,
        handler_receives_batch=True,
        init_consumer=init_worker,
    )

    merged = {key: int(value) for key, value in SHARED_STATS.items()}
    manager.shutdown()

    langs = sorted(
        key.split(":", 1)[0]
        for key in merged.keys()
        if ":" in key and not key.startswith("all:")
    )
    langs = sorted(set(langs))

    per_language = {
        lang: {
            "total_entities": int(merged.get("total_entities", 0)),
            "total_properties": int(merged.get("total_properties", 0)),
            "item_filter_pass": int(merged.get(f"{lang}:item", 0)),
            "scholarly_filter_pass": int(merged.get(f"{lang}:scholarly", 0)),
            "in_common": int(merged.get(f"{lang}:common", 0)),
        }
        for lang in langs
    }

    output = {
        "dump_path": DUMP_PATH,
        "languages": langs,
        "all_languages": {
            "total_entities": int(merged.get("total_entities", 0)),
            "total_properties": int(merged.get("total_properties", 0)),
            "item_filter_pass": int(merged.get("all:item", 0)),
            "scholarly_filter_pass": int(merged.get("all:scholarly", 0)),
            "in_common": int(merged.get("all:common", 0)),
        },
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
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    run_stats()
