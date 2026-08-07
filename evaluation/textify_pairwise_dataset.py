#!/usr/bin/env python3
"""Textify the correct and incorrect IDs in the pairwise evaluation CSV."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
from pathlib import Path

from tqdm import tqdm


def main() -> None:
    """Read IDs, textify them with main.py, and write one CSV per language."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, default=Path("evaluation/pairwise_evaluation_dataset.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("evaluation/textified"))
    parser.add_argument("--languages", default=os.environ.get("WD_LANGS", ""))
    parser.add_argument("--fallback-language", default=os.environ.get("FALLBACK_LANG"))
    args = parser.parse_args()

    with args.input_csv.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    ids = list(
        dict.fromkeys(
            [row["correct_id"] for row in rows] + [row["incorrect_id"] for row in rows]
        )
    )
    languages = [lang for lang in args.languages.split(",") if lang] or list(
        dict.fromkeys(row["language"] for row in rows)
    )

    from WikidataTextifier.src import get_wikibase_json_by_ids

    entities = {}
    for start in tqdm(range(0, len(ids), 50), desc="Fetching entities", unit="batch"):
        entities.update(
            get_wikibase_json_by_ids(
                ids[start : start + 50],
                props="labels|descriptions|aliases|claims",
            )
        )

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
