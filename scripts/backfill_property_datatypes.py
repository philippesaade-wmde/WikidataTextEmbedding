#!/usr/bin/env python3
"""Backfill AstraDB property datatypes from the configured Wikidata dump.

Example:
    WD_LANGS=en,de,fr,ar BACKFILL_APPLY=true \
        uv run python scripts/backfill_property_datatypes.py
"""

import os
import sys
from collections import defaultdict
from multiprocessing import get_context
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.WikidataDumpReader import WikidataDumpReader
from src.wikidataVectorDB import AstraDBConnect


# ---- Runtime config ----
READER_QUEUE_SIZE = int(os.environ.get("DATATYPE_BACKFILL_READER_QUEUE_SIZE", 8))
READER_BATCH_SIZE = int(os.environ.get("DATATYPE_BACKFILL_READER_BATCH_SIZE", 500))
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", 4))
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
LANG = os.environ.get("WD_LANG", os.environ.get("LANG", "en"))
WD_LANGS = tuple(lang.strip() for lang in os.environ.get("WD_LANGS", "").split(",") if lang.strip())
ASTRA_API_PATH = os.environ.get("ASTRA_API_PATH", "./API_tokens/datastax_api.json")
UPDATE_BATCH_SIZE = int(os.environ.get("DATATYPE_BACKFILL_BATCH_SIZE", 250))
APPLY = os.environ.get("BACKFILL_APPLY", "false").lower() == "true"
FORCE_DOWNLOAD_DUMP = os.environ.get("FORCE_DOWNLOAD_DUMP", "false").lower() == "true"


# ---- Process-local runtime state ----
ASTRADBS = {}
PROPERTY_COUNT = None
UPDATED_DOCUMENT_COUNT = None


def init_backfill_worker() -> None:
    """Create one AstraDB connection per target language in each reader worker."""
    global ASTRADBS

    if not APPLY:
        return
    languages = WD_LANGS or (LANG,)
    ASTRADBS = {
        lang: AstraDBConnect(lang=lang, entity_type="properties", config_path=ASTRA_API_PATH)
        for lang in languages
    }


def backfill_property_datatypes(items: list[dict]) -> None:
    """Backfill datatype metadata for property entities in one dump-reader batch."""
    pids_by_datatype: dict[str, list[str]] = defaultdict(list)
    for item in items:
        if item.get("type") != "property":
            continue

        pid = item.get("id")
        datatype = item.get("datatype")
        if isinstance(pid, str) and pid.startswith("P") and isinstance(datatype, str):
            pids_by_datatype[datatype].append(pid)

    with PROPERTY_COUNT.get_lock():
        PROPERTY_COUNT.value += sum(len(property_ids) for property_ids in pids_by_datatype.values())
    if not APPLY or not pids_by_datatype:
        return

    updated_documents = 0
    for astra in ASTRADBS.values():
        for datatype, property_ids in pids_by_datatype.items():
            for start in range(0, len(property_ids), UPDATE_BATCH_SIZE):
                property_batch = property_ids[start : start + UPDATE_BATCH_SIZE]
                result = astra.collection.update_many(
                    {"metadata.PID": {"$in": property_batch}},
                    {"$set": {"metadata.DataType": datatype}},
                )
                update_info = result.update_info or {}
                updated_documents += update_info.get("nModified", update_info.get("n", 0))

    with UPDATED_DOCUMENT_COUNT.get_lock():
        UPDATED_DOCUMENT_COUNT.value += updated_documents


def main() -> None:
    """Scan the dump once and update all configured property collections."""
    global PROPERTY_COUNT, UPDATED_DOCUMENT_COUNT

    if UPDATE_BATCH_SIZE < 1:
        raise ValueError("DATATYPE_BACKFILL_BATCH_SIZE must be positive.")

    context = get_context("fork")
    PROPERTY_COUNT = context.Value("i", 0)
    UPDATED_DOCUMENT_COUNT = context.Value("i", 0)

    languages = WD_LANGS or (LANG,)
    print(f"Reading {DUMP_PATH} once for property collections: {', '.join(languages)}")
    if not APPLY:
        print("Dry run. Set BACKFILL_APPLY=true to update AstraDB.")

    reader = WikidataDumpReader(
        DUMP_PATH,
        num_processes=NUM_PROCESSES,
        queue_size=READER_QUEUE_SIZE,
        batch_size=READER_BATCH_SIZE,
    )
    if FORCE_DOWNLOAD_DUMP or not os.path.exists(DUMP_PATH):
        print(f"Downloading Wikidata dump to {DUMP_PATH}.")
        reader.download()

    reader.run(
        backfill_property_datatypes,
        handler_receives_batch=True,
        init_consumer=init_backfill_worker,
    )

    print(f"Properties with datatypes: {PROPERTY_COUNT.value:,}")
    if APPLY:
        print(f"AstraDB documents updated: {UPDATED_DOCUMENT_COUNT.value:,}")


if __name__ == "__main__":
    main()
