#!/usr/bin/env python3
"""Score pairwise embedding outputs and summarize results by use case."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from contextlib import ExitStack
from dataclasses import dataclass
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
    from .embedding_models import MODEL_CLASSES, EmbeddingModel
except ImportError:  # Running this file directly: python evaluation/calculate_score.py
    from calculate_embeddings import (  # type: ignore[no-redef]
        DEFAULT_MODEL,
        OUTPUT_DIR,
        TEXTIFIED_DIR,
        USE_CASE_ROLES,
        load_existing,
        make_input_id,
    )
    from embedding_models import MODEL_CLASSES, EmbeddingModel  # type: ignore[no-redef]

RESULTS_DIR = Path("evaluation/results")
DEFAULT_OUTPUT = RESULTS_DIR / "embedding_scores.csv"
EPSILON = 1e-12
AVERAGED_FIELDS = (
    "accuracy",
    "mean_correct_similarity",
    "mean_incorrect_similarity",
    "mean_log_similarity_ratio",
)
LANGUAGE_MATCH = "same"
LANGUAGE_DIFFERENT = "diff"
LANGUAGE_SKIP_PREFIX = "__language_relation__"
AVERAGE_EMBEDDING_LANGUAGE = "avg"


def parse_texts(value: str) -> list[str]:
    """Parse textified chunks stored as a JSON list."""
    return [str(text) for text in json.loads(value or "[]") if text]


@dataclass(frozen=True)
class PairScore:
    """Similarity measurements for one query and candidate pair."""

    use_case: str
    query_language_matches: bool
    correct_similarity: float
    incorrect_similarity: float

    @property
    def accurate(self) -> bool:
        """Return whether the correct candidate is ranked higher."""
        return self.correct_similarity > self.incorrect_similarity

    @property
    def log_similarity_ratio(self) -> float:
        """Return the raw log similarity ratio, or NaN when it is undefined."""
        if self.correct_similarity < 0.0 or self.incorrect_similarity < 0.0:
            return float("nan")
        return float(np.log((self.correct_similarity + EPSILON) / (self.incorrect_similarity + EPSILON)))


def embedding_path(model_key: str, language: str) -> Path:
    """Return the output path written by calculate_embeddings.py."""
    model_folder = MODEL_CLASSES[model_key].output_folder
    return OUTPUT_DIR / model_folder / f"textified_to_embedding_{model_key}_{language}.npz"


def discover_languages(model_key: str) -> list[str]:
    """Find languages with an embedding output for one model."""
    model_folder = OUTPUT_DIR / MODEL_CLASSES[model_key].output_folder
    prefix = f"textified_to_embedding_{model_key}_"
    return sorted(
        path.stem.removeprefix(prefix) for path in model_folder.glob(f"{prefix}*.npz") if path.stem.startswith(prefix)
    )


def score_output(
    dataset_path: Path,
    output_path: Path,
    model_class: type[EmbeddingModel],
) -> tuple[list[PairScore], dict[str, int]]:
    """Score all complete rows in one model/language output."""
    arrays = load_existing(output_path)
    embeddings = arrays["embeddings"].astype(np.float32, copy=False)
    if embeddings.ndim != 2 or not len(embeddings):
        raise ValueError("Expected a non-empty two-dimensional embeddings array")
    index_by_id = {str(input_id): index for index, input_id in enumerate(arrays["input_ids"])}
    scores: list[PairScore] = []
    skipped: dict[str, int] = defaultdict(int)

    with dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            use_case = row.get("use_case", "")
            query = row.get("query", "")
            query_language = row.get("language", "")
            embedding_language = row.get("textification_language", "")
            correct_texts = parse_texts(row.get("correct_textified", ""))
            incorrect_texts = parse_texts(row.get("incorrect_textified", ""))
            if not query or not correct_texts or not incorrect_texts:
                continue
            if not query_language or not embedding_language:
                raise ValueError("Dataset rows must include language and textification_language")
            query_language_matches = query_language == embedding_language
            language_group = LANGUAGE_MATCH if query_language_matches else LANGUAGE_DIFFERENT

            query_role = USE_CASE_ROLES[use_case]
            query_text = model_class.prepare_text(query_role, query)
            query_id = make_input_id(query_role, query_text)
            correct_ids = [
                make_input_id("document", model_class.prepare_text("document", text)) for text in correct_texts
            ]
            incorrect_ids = [
                make_input_id("document", model_class.prepare_text("document", text)) for text in incorrect_texts
            ]
            required_ids = [query_id, *correct_ids, *incorrect_ids]
            if any(input_id not in index_by_id for input_id in required_ids):
                skipped[use_case] += 1
                skipped[f"{LANGUAGE_SKIP_PREFIX}{language_group}:{use_case}"] += 1
                continue

            query_index = index_by_id[query_id]
            query_embedding = embeddings[query_index]
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
                PairScore(
                    use_case=use_case,
                    query_language_matches=query_language_matches,
                    correct_similarity=correct_similarity,
                    incorrect_similarity=incorrect_similarity,
                )
            )

    return scores, dict(skipped)


def load_embedding_vectors(path: Path) -> tuple[dict[str, int], np.ndarray]:
    """Load only IDs and vectors needed to score an embedding output."""
    with np.load(path, allow_pickle=False) as data:
        missing = {"input_ids", "embeddings"} - set(data.files)
        if missing:
            raise ValueError(f"{path} is missing fields: {sorted(missing)}")
        input_ids = data["input_ids"].astype(str)
        embeddings = data["embeddings"].astype(np.float32, copy=False)
    if embeddings.ndim != 2 or embeddings.shape[0] != len(input_ids):
        raise ValueError(f"Embeddings in {path} have an invalid shape")
    index_by_id = {input_id: index for index, input_id in enumerate(input_ids)}
    if len(index_by_id) != len(input_ids):
        raise ValueError(f"Duplicate input IDs in {path}")
    return index_by_id, embeddings


def score_average_embeddings(
    dataset_paths: dict[str, Path],
    output_paths: dict[str, Path],
    model_class: type[EmbeddingModel],
) -> tuple[list[PairScore], dict[str, int]]:
    """Score embeddings averaged equally across all selected languages."""
    languages = list(dataset_paths)
    if len(languages) < 2 or set(languages) != set(output_paths):
        raise ValueError("Average embedding evaluation requires the same two or more languages")

    outputs = {language: load_embedding_vectors(output_paths[language]) for language in languages}
    dimensions = {embeddings.shape[1] for _, embeddings in outputs.values()}
    if len(dimensions) != 1:
        raise ValueError("Cannot average embeddings with different dimensions")

    scores: list[PairScore] = []
    skipped: dict[str, int] = defaultdict(int)
    identity_fields = (
        "source",
        "query",
        "language",
        "correct_id",
        "incorrect_id",
        "use_case",
    )

    with ExitStack() as stack:
        readers = [
            csv.DictReader(stack.enter_context(dataset_paths[language].open(encoding="utf-8", newline="")))
            for language in languages
        ]
        for row_number, language_rows in enumerate(zip(*readers, strict=True), start=2):
            first_row = language_rows[0]
            identity = tuple(first_row.get(field, "") for field in identity_fields)
            if any(tuple(row.get(field, "") for field in identity_fields) != identity for row in language_rows[1:]):
                raise ValueError(f"Textified datasets differ at row {row_number}")

            use_case = first_row.get("use_case", "")
            query = first_row.get("query", "")
            if not query:
                continue
            query_role = USE_CASE_ROLES[use_case]
            query_text = model_class.prepare_text(query_role, query)
            query_id = make_input_id(query_role, query_text)
            query_embeddings: list[np.ndarray] = []
            correct_embeddings: list[np.ndarray] = []
            incorrect_embeddings: list[np.ndarray] = []
            complete = True

            for language, row in zip(languages, language_rows, strict=True):
                if row.get("textification_language", "") != language:
                    raise ValueError(f"Expected textification language {language!r} at row {row_number}")
                correct_texts = parse_texts(row.get("correct_textified", ""))
                incorrect_texts = parse_texts(row.get("incorrect_textified", ""))
                if not correct_texts or not incorrect_texts:
                    complete = False
                    break
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
                required_ids = [query_id, *correct_ids, *incorrect_ids]
                if any(input_id not in index_by_id for input_id in required_ids):
                    complete = False
                    break
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

            if not complete:
                skipped[use_case] += 1
                continue

            average_query = np.mean(query_embeddings, axis=0)
            average_correct = np.mean(correct_embeddings, axis=0)
            average_incorrect = np.mean(incorrect_embeddings, axis=0)
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
            scores.append(
                PairScore(
                    use_case=use_case,
                    query_language_matches=False,
                    correct_similarity=correct_similarity,
                    incorrect_similarity=incorrect_similarity,
                )
            )

    return scores, dict(skipped)


def aggregate_scores(
    model_key: str,
    language: str,
    scores: list[PairScore],
    skipped: dict[str, int],
) -> list[dict[str, str | int | float]]:
    """Aggregate pair scores by use case and add an overall row."""
    by_use_case: dict[str, list[PairScore]] = defaultdict(list)
    for score in scores:
        by_use_case[score.use_case].append(score)

    regular_skipped = {key for key in skipped if not key.startswith(LANGUAGE_SKIP_PREFIX)}
    regular_use_cases = sorted(set(by_use_case) | regular_skipped)
    groups = [
        *((use_case, by_use_case[use_case]) for use_case in regular_use_cases),
        ("overall", scores),
    ]

    rows: list[dict[str, str | int | float]] = []
    for use_case, group in groups:
        if use_case == "overall":
            group_skipped = sum(skipped.get(key, 0) for key in regular_use_cases)
        else:
            group_skipped = skipped.get(use_case, 0)
        rows.append(make_score_row(model_key, language, use_case, group, group_skipped))
    return rows


def make_score_row(
    model_key: str,
    language: str,
    use_case: str,
    scores: list[PairScore],
    skipped: int,
) -> dict[str, str | int | float]:
    """Aggregate individual query scores into one table row."""
    if not scores:
        accuracy = mean_correct = mean_incorrect = mean_log_ratio = float("nan")
    else:
        accuracy = sum(score.accurate for score in scores) / len(scores)
        mean_correct = float(np.mean([score.correct_similarity for score in scores]))
        mean_incorrect = float(np.mean([score.incorrect_similarity for score in scores]))
        log_ratios = [score.log_similarity_ratio for score in scores]
        finite_log_ratios = [ratio for ratio in log_ratios if np.isfinite(ratio)]
        mean_log_ratio = float(np.mean(finite_log_ratios)) if finite_log_ratios else float("nan")
    return {
        "model": model_key,
        "language": language,
        "use_case": use_case,
        "evaluated": len(scores),
        "skipped": skipped,
        "accuracy": accuracy,
        "mean_correct_similarity": mean_correct,
        "mean_incorrect_similarity": mean_incorrect,
        "mean_log_similarity_ratio": mean_log_ratio,
    }


def aggregate_language_groups(
    model_key: str,
    scores: list[PairScore],
    skipped: dict[str, int],
) -> list[dict[str, str | int | float]]:
    """Pool queries into same- and different-language table sections."""
    rows: list[dict[str, str | int | float]] = []
    for language, matches in (
        (LANGUAGE_MATCH, True),
        (LANGUAGE_DIFFERENT, False),
    ):
        prefix = f"{LANGUAGE_SKIP_PREFIX}{language}:"
        language_skipped = {key.removeprefix(prefix): count for key, count in skipped.items() if key.startswith(prefix)}
        rows.extend(
            aggregate_scores(
                model_key,
                language,
                [score for score in scores if score.query_language_matches == matches],
                language_skipped,
            )
        )
    return rows


def average_language_scores(
    rows: list[dict[str, str | int | float]],
) -> list[dict[str, str | int | float]]:
    """Macro-average each model's score rows across two or more languages."""
    by_model: dict[str, list[dict[str, str | int | float]]] = defaultdict(list)
    for row in rows:
        by_model[str(row["model"])].append(row)

    averages: list[dict[str, str | int | float]] = []
    for model, model_rows in by_model.items():
        languages = {str(row["language"]) for row in model_rows}
        if len(languages) < 2:
            continue
        use_cases = {str(row["use_case"]) for row in model_rows}
        regular_use_cases = use_cases - {"overall"}
        ordered_use_cases = [*sorted(regular_use_cases), "overall"]
        for use_case in ordered_use_cases:
            group = [row for row in model_rows if row["use_case"] == use_case]
            average: dict[str, str | int | float] = {
                "model": model,
                "language": "all",
                "use_case": use_case,
                "evaluated": sum(int(row["evaluated"]) for row in group),
                "skipped": sum(int(row["skipped"]) for row in group),
            }
            for field in AVERAGED_FIELDS:
                values = [float(row[field]) for row in group if np.isfinite(float(row[field]))]
                average[field] = float(np.mean(values)) if values else float("nan")
            averages.append(average)
    return averages


def format_number(value: object, *, percentage: bool = False) -> str:
    """Format a numeric table value compactly."""
    number = float(value)
    if np.isnan(number):
        return "n/a"
    if percentage:
        return f"{number:.2%}"
    return f"{number:.4f}"


def print_table(rows: list[dict[str, str | int | float]]) -> None:
    """Print aggregate score rows as a simple terminal table."""
    headers = ("Model", "Lang", "Use case", "N", "Skipped", "Accuracy", "Correct", "Incorrect", "Log ratio")
    rendered = [
        (
            str(row["model"]),
            str(row["language"]),
            str(row["use_case"]),
            str(row["evaluated"]),
            str(row["skipped"]),
            format_number(row["accuracy"], percentage=True),
            format_number(row["mean_correct_similarity"]),
            format_number(row["mean_incorrect_similarity"]),
            format_number(row["mean_log_similarity_ratio"]),
        )
        for row in rows
    ]
    widths = [max(len(header), *(len(row[index]) for row in rendered)) for index, header in enumerate(headers)]
    print("  ".join(header.ljust(width) for header, width in zip(headers, widths, strict=True)))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(value.ljust(width) for value, width in zip(row, widths, strict=True)))


def save_table(path: Path, rows: list[dict[str, str | int | float]]) -> None:
    """Save aggregate score rows as CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


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
        languages = list(dict.fromkeys(args.language or discover_languages(model_key)))
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
    scores_by_model: dict[str, list[PairScore]] = defaultdict(list)
    skipped_by_model: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    languages_by_model: dict[str, set[str]] = defaultdict(set)
    for model_key, language in pairs:
        scores, skipped = score_output(
            TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv",
            embedding_path(model_key, language),
            MODEL_CLASSES[model_key],
        )
        table_rows.extend(aggregate_scores(model_key, language, scores, skipped))
        scores_by_model[model_key].extend(scores)
        languages_by_model[model_key].add(language)
        for group, count in skipped.items():
            skipped_by_model[model_key][group] += count

    average_rows = average_language_scores(table_rows)
    for model_key in models:
        model_average_rows = [row for row in average_rows if row["model"] == model_key]
        model_languages = sorted(languages_by_model[model_key])
        if len(model_languages) >= 2:
            average_embedding_scores, average_embedding_skipped = score_average_embeddings(
                {
                    language: TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv"
                    for language in model_languages
                },
                {language: embedding_path(model_key, language) for language in model_languages},
                MODEL_CLASSES[model_key],
            )
            table_rows.extend(
                aggregate_scores(
                    model_key,
                    AVERAGE_EMBEDDING_LANGUAGE,
                    average_embedding_scores,
                    average_embedding_skipped,
                )
            )
        if len(model_languages) >= 2:
            table_rows.extend(
                aggregate_language_groups(
                    model_key,
                    scores_by_model[model_key],
                    skipped_by_model[model_key],
                )
            )
        table_rows.extend(model_average_rows)

    print_table(table_rows)
    save_table(args.output, table_rows)
    print(f"\nSaved results to {args.output}")


if __name__ == "__main__":
    main()
