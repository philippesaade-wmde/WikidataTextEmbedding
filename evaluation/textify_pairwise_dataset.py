#!/usr/bin/env python3
"""Textify the correct and incorrect IDs in the pairwise evaluation CSV."""

from __future__ import annotations

import argparse
import copy
import csv
import gzip
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from tqdm import tqdm


WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
USER_AGENT = "WikidataTextEmbedding pairwise evaluation textifier"


def read_pairwise_rows(path: Path) -> list[dict[str, str]]:
    """Read a real CSV, or a gzip-compressed Gnumeric workbook mislabeled as CSV."""
    if path.read_bytes()[:2] != b"\x1f\x8b":
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    with gzip.open(path, "rb") as handle:
        root = ET.parse(handle).getroot()

    cells: dict[tuple[int, int], str] = {}
    max_row = 0
    max_col = 0
    for cell in root.iter():
        if not cell.tag.endswith("Cell"):
            continue
        row = int(cell.attrib["Row"])
        col = int(cell.attrib["Col"])
        cells[(row, col)] = cell.text or ""
        max_row = max(max_row, row)
        max_col = max(max_col, col)

    header = [cells.get((0, col), "") for col in range(max_col + 1)]
    return [
        dict(zip(header, [cells.get((row, col), "") for col in range(max_col + 1)]))
        for row in range(1, max_row + 1)
    ]


def fetch_wikidata_entities(
    ids: list[str],
    *,
    languages: list[str],
    batch_size: int = 50,
    max_retries: int = 5,
    retry_sleep_seconds: float = 5.0,
) -> dict[str, dict]:
    """Fetch full entity JSON from the public Wikidata API."""
    entities: dict[str, dict] = {}
    language_param = "|".join(dict.fromkeys([*languages, "mul", "en"]))

    for start in tqdm(range(0, len(ids), batch_size), desc="Fetching entities", unit="batch"):
        batch = ids[start : start + batch_size]
        params = {
            "action": "wbgetentities",
            "format": "json",
            "formatversion": "2",
            "ids": "|".join(batch),
            "props": "info|labels|descriptions|aliases|claims|sitelinks|datatype",
            "languages": language_param,
        }
        url = f"{WIKIDATA_API_URL}?{urllib.parse.urlencode(params)}"

        for attempt in range(max_retries + 1):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(request, timeout=60) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except Exception:
                if attempt == max_retries:
                    raise
                time.sleep(retry_sleep_seconds * (attempt + 1))

        entities.update(
            {
                entity_id: entity
                for entity_id, entity in payload.get("entities", {}).items()
                if not entity.get("missing")
            }
        )

    missing = sorted(set(ids) - set(entities))
    if missing:
        raise ValueError(f"Wikidata API did not return {len(missing)} IDs, first missing IDs: {missing[:20]}")

    return entities


def main() -> None:
    """Read IDs, textify them with main.py, and write one CSV per language."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, default=Path("evaluation/pairwise_evaluation_dataset.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("evaluation/textified"))
    parser.add_argument("--languages", default=os.environ.get("WD_LANGS", ""))
    parser.add_argument("--fallback-language", default=os.environ.get("FALLBACK_LANG"))
    args = parser.parse_args()

    rows = read_pairwise_rows(args.input_csv)
    if not rows:
        raise ValueError(f"No rows found in {args.input_csv}")

    ids = list(
        dict.fromkeys(
            [row["correct_id"] for row in rows] + [row["incorrect_id"] for row in rows]
        )
    )
    languages = [lang for lang in args.languages.split(",") if lang] or list(
        dict.fromkeys(row["language"] for row in rows)
    )

    entities = fetch_wikidata_entities(ids, languages=languages)

    import main as pipeline_main
    from main import item_to_text

    pipeline_main.check_wdtextifier_stack()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for language in languages:
        pipeline_main.LANG = language
        pipeline_main.FALLBACK_LANG = args.fallback_language or os.environ.get(
            f"FALLBACK_LANG_{language.upper()}", language
        )
        pipeline_main.reset_runtime_state()
        pipeline_main.init_worker(enable_vector=False)
        labels = pipeline_main.LazyLabelFactory(
            lang=language,
            fallback_lang=pipeline_main.FALLBACK_LANG,
        )

        chunks = {}
        for entity_id, entity in tqdm(entities.items(), desc=f"Textifying {language}", unit="entity"):
            chunks[entity_id] = [
                document["content"]
                for document in item_to_text(copy.deepcopy(entity), label_factory=labels)
            ]

        output = args.output_dir / f"{args.input_csv.stem}_textified_{language}.csv"
        fields = list(rows[0]) + ["textification_language", "correct_textified", "incorrect_textified"]
        fields = list(dict.fromkeys(fields))
        with output.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for source in rows:
                row = dict(source)
                row["textification_language"] = language
                row["correct_textified"] = json.dumps(chunks[row["correct_id"]], ensure_ascii=False)
                row["incorrect_textified"] = json.dumps(chunks[row["incorrect_id"]], ensure_ascii=False)
                writer.writerow(row)
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
