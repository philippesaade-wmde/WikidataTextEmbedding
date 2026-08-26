#!/usr/bin/env python3
"""Score pairwise embedding outputs and summarize results by use case."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

import numpy as np

try:
    from .calculate_embeddings import (
        DEFAULT_MODEL,
        OUTPUT_DIR,
        TEXTIFIED_DIR,
        USE_CASE_ROLES,
        load_existing,
        make_input_id,
    )
    from .embedding_models import MODEL_CLASSES
except ImportError:  # Running this file directly: python evaluation/calculate_score.py
    from calculate_embeddings import (  # type: ignore[no-redef]
        DEFAULT_MODEL,
        OUTPUT_DIR,
        TEXTIFIED_DIR,
        USE_CASE_ROLES,
        load_existing,
        make_input_id,
    )
    from embedding_models import MODEL_CLASSES  # type: ignore[no-redef]

DEFAULT_OUTPUT = Path("evaluation/results/embedding_scores.csv")


def parse_texts(value: str) -> list[str]:
    """Parse textified chunks stored as a JSON list."""
    return [str(text) for text in json.loads(value or "[]") if text]


def embedding_path(model_key: str, language: str) -> Path:
    """Return the output path written by calculate_embeddings.py."""
    model_folder = MODEL_CLASSES[model_key].output_folder
    return OUTPUT_DIR / model_folder / f"textified_to_embedding_{model_key}_{language}.npz"


def score_language(model_key: str, language: str) -> list[tuple[str, bool, bool]]:
    """Score all complete rows in one model/language output."""
    model_class = MODEL_CLASSES[model_key]
    arrays = load_existing(embedding_path(model_key, language))
    embeddings = arrays["embeddings"].astype(np.float32, copy=False)
    index_by_id = {str(input_id): index for index, input_id in enumerate(arrays["input_ids"])}
    scores: list[tuple[str, bool, bool]] = []

    dataset_path = TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv"
    with dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            use_case = row["use_case"]
            query_role = USE_CASE_ROLES[use_case]
            query_text = model_class.prepare_text(query_role, row["query"])
            query_id = make_input_id(query_role, query_text)
            correct_texts = parse_texts(row["correct_textified"])
            incorrect_texts = parse_texts(row["incorrect_textified"])
            correct_ids = [
                make_input_id("document", model_class.prepare_text("document", text)) for text in correct_texts
            ]
            incorrect_ids = [
                make_input_id("document", model_class.prepare_text("document", text)) for text in incorrect_texts
            ]
            query_embedding = embeddings[index_by_id[query_id]]
            correct_similarity = float(
                np.max(
                    model_class.calculate_similarity(
                        query_embedding,
                        embeddings[[index_by_id[input_id] for input_id in correct_ids]],
                    )
                )
            )
            incorrect_similarity = float(
                np.max(
                    model_class.calculate_similarity(
                        query_embedding,
                        embeddings[[index_by_id[input_id] for input_id in incorrect_ids]],
                    )
                )
            )
            scores.append(
                (
                    use_case,
                    row["language"] == row["textification_language"],
                    correct_similarity > incorrect_similarity,
                )
            )

    return scores


def score_average_embeddings(
    model_key: str,
    languages: list[str],
) -> list[tuple[str, bool, bool]]:
    """Score embeddings averaged equally across all selected languages."""
    model_class = MODEL_CLASSES[model_key]
    outputs: dict[str, tuple[dict[str, int], np.ndarray]] = {}
    for language in languages:
        arrays = load_existing(embedding_path(model_key, language))
        index_by_id = {str(input_id): index for index, input_id in enumerate(arrays["input_ids"])}
        outputs[language] = (index_by_id, arrays["embeddings"].astype(np.float32, copy=False))

    scores: list[tuple[str, bool, bool]] = []

    with ExitStack() as stack:
        readers = [
            csv.DictReader(
                stack.enter_context(
                    (TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv").open(
                        encoding="utf-8",
                        newline="",
                    )
                )
            )
            for language in languages
        ]
        for language_rows in zip(*readers):
            first_row = language_rows[0]
            use_case = first_row["use_case"]
            query_role = USE_CASE_ROLES[use_case]
            query_text = model_class.prepare_text(query_role, first_row["query"])
            query_id = make_input_id(query_role, query_text)
            query_embeddings: list[np.ndarray] = []
            correct_embeddings: list[np.ndarray] = []
            incorrect_embeddings: list[np.ndarray] = []

            for language, row in zip(languages, language_rows):
                correct_texts = parse_texts(row["correct_textified"])
                incorrect_texts = parse_texts(row["incorrect_textified"])
                correct_ids = [
                    make_input_id(
                        "document",
                        model_class.prepare_text("document", text),
                    )
                    for text in correct_texts
                ]
                incorrect_ids = [
                    make_input_id(
                        "document",
                        model_class.prepare_text("document", text),
                    )
                    for text in incorrect_texts
                ]
                index_by_id, embeddings = outputs[language]
                query_embeddings.append(embeddings[index_by_id[query_id]])
                correct_embeddings.append(
                    np.mean(
                        embeddings[[index_by_id[input_id] for input_id in correct_ids]],
                        axis=0,
                    )
                )
                incorrect_embeddings.append(
                    np.mean(
                        embeddings[[index_by_id[input_id] for input_id in incorrect_ids]],
                        axis=0,
                    )
                )

            average_query = np.mean(query_embeddings, axis=0)
            average_correct = np.mean(correct_embeddings, axis=0)
            average_incorrect = np.mean(incorrect_embeddings, axis=0)
            average_query /= np.linalg.norm(average_query)
            average_correct /= np.linalg.norm(average_correct)
            average_incorrect /= np.linalg.norm(average_incorrect)
            correct_similarity = float(
                model_class.calculate_similarity(
                    average_query,
                    average_correct.reshape(1, -1),
                )[0]
            )
            incorrect_similarity = float(
                model_class.calculate_similarity(
                    average_query,
                    average_incorrect.reshape(1, -1),
                )[0]
            )
            scores.append((use_case, False, correct_similarity > incorrect_similarity))

    return scores


def summarize(
    model_key: str,
    language: str,
    scores: list[tuple[str, bool, bool]],
) -> list[dict[str, str | int | float]]:
    """Summarize pair accuracy by use case and add a balanced overall row."""
    by_use_case: dict[str, list[bool]] = defaultdict(list)
    for use_case, _, accurate in scores:
        by_use_case[use_case].append(accurate)

    rows: list[dict[str, str | int | float]] = []
    for use_case, results in sorted(by_use_case.items()):
        rows.append(
            {
                "model": model_key,
                "language": language,
                "use_case": use_case,
                "evaluated": len(results),
                "accuracy": sum(results) / len(results),
            }
        )

    rows.append(
        {
            "model": model_key,
            "language": language,
            "use_case": "overall",
            "evaluated": sum(int(row["evaluated"]) for row in rows),
            "accuracy": sum(float(row["accuracy"]) for row in rows) / len(rows),
        }
    )
    return rows


def main() -> None:
    """Score selected model outputs, print a table, and save it as CSV."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=sorted(MODEL_CLASSES), action="append")
    parser.add_argument("--language", action="append")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    models = list(dict.fromkeys(args.model or [DEFAULT_MODEL]))
    pairs: list[tuple[str, str]] = []
    for model_key in models:
        model_folder = OUTPUT_DIR / MODEL_CLASSES[model_key].output_folder
        prefix = f"textified_to_embedding_{model_key}_"
        discovered_languages = sorted(path.stem.removeprefix(prefix) for path in model_folder.glob(f"{prefix}*.npz"))
        languages = list(dict.fromkeys(args.language or discovered_languages))
        if not languages:
            parser.error(f"No embedding outputs found for model {model_key}")
        for language in languages:
            output_path = embedding_path(model_key, language)
            dataset_path = TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv"
            if not output_path.exists():
                parser.error(f"Embedding output not found: {output_path}")
            if not dataset_path.exists():
                parser.error(f"Textified dataset not found: {dataset_path}")
            pairs.append((model_key, language))

    table_rows: list[dict[str, str | int | float]] = []
    scores_by_model: dict[str, list[tuple[str, bool, bool]]] = defaultdict(list)
    languages_by_model: dict[str, set[str]] = defaultdict(set)
    for model_key, language in pairs:
        scores = score_language(model_key, language)
        table_rows.extend(summarize(model_key, language, scores))
        scores_by_model[model_key].extend(scores)
        languages_by_model[model_key].add(language)

    for model_key in models:
        model_languages = sorted(languages_by_model[model_key])
        if len(model_languages) >= 2:
            scores = scores_by_model[model_key]
            table_rows.extend(summarize(model_key, "avg", score_average_embeddings(model_key, model_languages)))
            table_rows.extend(summarize(model_key, "same", [score for score in scores if score[1]]))
            table_rows.extend(summarize(model_key, "diff", [score for score in scores if not score[1]]))
            table_rows.extend(summarize(model_key, "all", scores))

    headers = ("Model", "Lang", "Use case", "N", "Accuracy")
    rendered = [
        (
            str(row["model"]),
            str(row["language"]),
            str(row["use_case"]),
            str(row["evaluated"]),
            f"{float(row['accuracy']):.2%}",
        )
        for row in table_rows
    ]
    widths = [max(len(header), *(len(row[index]) for row in rendered)) for index, header in enumerate(headers)]
    print("  ".join(header.ljust(width) for header, width in zip(headers, widths, strict=True)))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(value.ljust(width) for value, width in zip(row, widths, strict=True)))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(table_rows[0]))
        writer.writeheader()
        writer.writerows(table_rows)
    print(f"\nSaved results to {args.output}")


if __name__ == "__main__":
    main()
