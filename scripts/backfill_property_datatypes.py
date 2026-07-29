#!/usr/bin/env python3
"""Backfill AstraDB property datatypes from the configured Wikidata dump.

Example:
    WD_LANGS=en,de,fr,ar BACKFILL_APPLY=true \
        uv run python scripts/backfill_property_datatypes.py
"""

import gzip
import os
import sys
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.WikidataDumpReader import WikidataDumpReader
from src.wikidataVectorDB import AstraDBConnect


# ---- Runtime config ----
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
LANG = os.environ.get("WD_LANG", os.environ.get("LANG", "en"))
WD_LANGS = tuple(lang.strip() for lang in os.environ.get("WD_LANGS", "").split(",") if lang.strip())
ASTRA_API_PATH = os.environ.get("ASTRA_API_PATH", "./API_tokens/datastax_api.json")
UPDATE_BATCH_SIZE = int(os.environ.get("DATATYPE_BACKFILL_BATCH_SIZE", 250))
APPLY = os.environ.get("BACKFILL_APPLY", "false").lower() == "true"


def batched(values: list[str], size: int) -> Iterator[list[str]]:
    """Yield fixed-size batches from values."""
    for start in range(0, len(values), size):
        yield values[start : start + size]


def collect_property_datatypes() -> dict[str, list[str]]:
    """Read every property datatype from the configured Wikidata dump."""
    dump_path = Path(DUMP_PATH)
    if not dump_path.is_file():
        raise FileNotFoundError(f"Dump file not found: {dump_path}")
    if dump_path.suffix != ".gz":
        raise ValueError(f"Expected a .gz Wikidata dump, got: {dump_path}")

    reader = WikidataDumpReader(DUMP_PATH, num_processes=1)
    pids_by_datatype: dict[str, list[str]] = defaultdict(list)

    try:
        with gzip.open(dump_path, mode="rt", encoding="utf-8") as dump_file:
            for line in dump_file:
                entity = reader.line_to_entity(line)
                if not entity or entity.get("type") != "property":
                    continue

                pid = entity.get("id")
                datatype = entity.get("datatype")
                if isinstance(pid, str) and pid.startswith("P") and isinstance(datatype, str):
                    pids_by_datatype[datatype].append(pid)
    except (EOFError, OSError) as error:
        raise RuntimeError(
            f"Could not read a complete Wikidata dump from {dump_path}. "
            "Download the full dump before running this backfill."
        ) from error

    if not pids_by_datatype:
        raise RuntimeError("No property datatypes were found in the dump.")
    return dict(pids_by_datatype)


def backfill_language(lang: str, pids_by_datatype: dict[str, list[str]]) -> int:
    """Set metadata.DataType for all property chunks in one language collection."""
    astra = AstraDBConnect(lang=lang, config_path=ASTRA_API_PATH)
    updated_documents = 0

    for datatype, property_ids in sorted(pids_by_datatype.items()):
        for property_batch in batched(property_ids, UPDATE_BATCH_SIZE):
            result = astra.property_collection.update_many(
                {"metadata.PID": {"$in": property_batch}},
                {"$set": {"metadata.DataType": datatype}},
            )
            updated_documents += result.update_info.get("nModified", 0)

    return updated_documents


def run_backfill() -> None:
    """Load dump datatypes, then optionally write them to each property collection."""
    if UPDATE_BATCH_SIZE < 1:
        raise ValueError("DATATYPE_BACKFILL_BATCH_SIZE must be positive.")

    pids_by_datatype = collect_property_datatypes()
    property_count = sum(len(property_ids) for property_ids in pids_by_datatype.values())
    print(f"Loaded {property_count:,} properties across {len(pids_by_datatype)} datatypes.")

    languages = WD_LANGS or (LANG,)
    if not APPLY:
        print(f"Dry run for property collections: {', '.join(languages)}")
        print("Set BACKFILL_APPLY=true to update AstraDB.")
        return

    for lang in languages:
        updated_documents = backfill_language(lang, pids_by_datatype)
        print(f"{lang}: updated {updated_documents:,} property documents.")


if __name__ == "__main__":
    run_backfill()
