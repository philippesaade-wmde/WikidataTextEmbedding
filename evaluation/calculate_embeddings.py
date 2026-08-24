#!/usr/bin/env python3
"""Calculate and resume text embeddings for the textified evaluation data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm

try:
    from .embedding_models import MODEL_CLASSES, EmbeddingModel
    from .embedding_models.base import Role
except ImportError:  # Running this file directly: python evaluation/calculate_embeddings.py
    from embedding_models import MODEL_CLASSES, EmbeddingModel
    from embedding_models.base import Role

load_dotenv()

csv.field_size_limit(sys.maxsize)

TEXTIFIED_DIR = Path("evaluation/textified")
DATASET_PATTERN = "pairwise_evaluation_dataset_textified_*.csv"
OUTPUT_DIR = Path("evaluation/embeddings")
DEFAULT_MODEL = "jina-v3"
FIELDS = ("input_ids", "roles", "wikidata_ids", "textified", "source_modes", "embeddings")
USE_CASE_ROLES: dict[str, Role] = {
    "question_answering": "question",
    "entity_linking": "entity_linking",
    "disambiguation": "disambiguation",
    "property_linking": "property_linking",
}


def make_input_id(role: str, text: str) -> str:
    """Return the stable ID for a role and submitted text."""
    return hashlib.sha256(f"{role}\0{text}".encode("utf-8")).hexdigest()


def load_inputs(path: Path, model_class: type[EmbeddingModel]) -> list[dict[str, str]]:
    """Read and deduplicate query, correct, and incorrect texts from one CSV."""
    inputs: dict[str, dict[str, str]] = {}

    def add(role: Role, text: str, wikidata_id: str = "") -> None:
        submitted = model_class.prepare_text(role, text)
        input_id = make_input_id(role, submitted)
        existing = inputs.get(input_id)
        if existing and existing["textified"] != submitted:
            raise ValueError(f"Input ID collision for {input_id}")
        if existing is None:
            inputs[input_id] = {
                "input_id": input_id,
                "role": role,
                "wikidata_id": wikidata_id,
                "textified": submitted,
            }

    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if not row.get("query"):
                continue

            correct = json.loads(row.get("correct_textified", "") or "[]")
            incorrect = json.loads(row.get("incorrect_textified", "") or "[]")
            correct = [str(text) for text in correct if text]
            incorrect = [str(text) for text in incorrect if text]
            if not correct or not incorrect:
                continue

            query_role = USE_CASE_ROLES[row.get("use_case", "")]
            add(query_role, row["query"])
            for text in correct:
                add("document", text, row.get("correct_id", ""))
            for text in incorrect:
                add("document", text, row.get("incorrect_id", ""))

    return list(inputs.values())


def load_existing(path: Path) -> dict[str, np.ndarray]:
    """Load an existing output file, or return empty arrays."""
    if not path.exists():
        return {
            "input_ids": np.asarray([], dtype="U64"),
            "roles": np.asarray([], dtype="U32"),
            "wikidata_ids": np.asarray([], dtype="U32"),
            "textified": np.asarray([], dtype="U1"),
            "source_modes": np.asarray([], dtype="U10"),
            "embeddings": np.empty((0, 0), dtype=np.float32),
        }

    with np.load(path, allow_pickle=False) as data:
        missing = set(FIELDS) - set(data.files)
        if missing:
            raise ValueError(f"{path} is missing fields: {sorted(missing)}")
        arrays = {field: data[field] for field in FIELDS}

    # Outputs created before roles were standardized used correct/incorrect.
    # Their document vectors are still valid, so migrate their IDs instead of
    # embedding those passages again.
    roles = arrays["roles"].astype("U32")
    legacy_documents = np.isin(roles, ["correct", "incorrect"])
    if legacy_documents.any():
        arrays["roles"] = np.where(legacy_documents, "document", roles)
        arrays["input_ids"] = np.asarray(
            [
                make_input_id("document", text) if legacy else input_id
                for input_id, text, legacy in zip(
                    arrays["input_ids"], arrays["textified"], legacy_documents, strict=True
                )
            ],
            dtype="U64",
        )

        # Correct and incorrect entries with identical text now share one
        # document role, so keep the first copy of each canonical ID.
        _, keep_indices = np.unique(arrays["input_ids"], return_index=True)
        keep = np.zeros(len(arrays["input_ids"]), dtype=bool)
        keep[keep_indices] = True
        arrays = {field: values[keep] for field, values in arrays.items()}
    else:
        arrays["roles"] = roles

    count = len(arrays["input_ids"])
    if any(len(arrays[field]) != count for field in FIELDS[:-1]):
        raise ValueError(f"Metadata arrays in {path} have different lengths")
    if arrays["embeddings"].ndim != 2 or arrays["embeddings"].shape[0] != count:
        raise ValueError(f"Embeddings in {path} have an invalid shape")
    if len(set(map(str, arrays["input_ids"]))) != count:
        raise ValueError(f"Duplicate input IDs in {path}")
    return arrays


def remove_stale_inputs(
    arrays: dict[str, np.ndarray],
    inputs: list[dict[str, str]],
) -> tuple[dict[str, np.ndarray], int]:
    """Drop embeddings whose prompted input is no longer in the current dataset."""
    current_ids = {item["input_id"] for item in inputs}
    keep = np.asarray(
        [str(input_id) in current_ids for input_id in arrays["input_ids"]],
        dtype=bool,
    )
    stale = int((~keep).sum())
    if stale:
        arrays = {field: values[keep] for field, values in arrays.items()}
    return arrays, stale


def save_output(path: Path, arrays: dict[str, np.ndarray]) -> None:
    """Atomically write one compressed output file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.npz")
    np.savez_compressed(temporary, **arrays)
    os.replace(temporary, path)


def append_batch(
    arrays: dict[str, np.ndarray],
    inputs: list[dict[str, str]],
    vectors: np.ndarray,
) -> None:
    """Append one successful batch to the output arrays."""
    metadata = {
        "input_ids": np.asarray([item["input_id"] for item in inputs]),
        "roles": np.asarray([item["role"] for item in inputs]),
        "wikidata_ids": np.asarray([item["wikidata_id"] for item in inputs]),
        "textified": np.asarray([item["textified"] for item in inputs]),
        "source_modes": np.asarray(["batch"] * len(inputs)),
    }
    if arrays["embeddings"].shape[0] == 0:
        arrays["embeddings"] = vectors.astype(np.float32)
    else:
        arrays["embeddings"] = np.vstack((arrays["embeddings"], vectors)).astype(np.float32)
    for field, values in metadata.items():
        arrays[field] = np.concatenate((arrays[field], values))


def process(model_key: str, language: str, batch_size: int, timeout: int, dry_run: bool) -> None:
    """Embed all missing inputs for one model and language."""
    model_class = MODEL_CLASSES[model_key]
    model_folder = model_class.output_folder
    path = OUTPUT_DIR / model_folder / f"textified_to_embedding_{model_key}_{language}.npz"

    dataset_path = TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv"
    inputs = load_inputs(dataset_path, model_class)
    arrays = load_existing(path)
    arrays, stale = remove_stale_inputs(arrays, inputs)
    existing_text = dict(zip(arrays["input_ids"].astype(str), arrays["textified"].astype(str), strict=True))

    pending: list[dict[str, str]] = []
    for item in inputs:
        saved_text = existing_text.get(item["input_id"])
        if saved_text is None:
            pending.append(item)
        elif saved_text != item["textified"]:
            raise ValueError(f"Text changed for existing input {item['input_id']}")

    print(
        f"{model_key}/{language}: {len(inputs)} total, {len(existing_text)} existing, "
        f"{len(pending)} pending, {stale} stale"
    )
    if dry_run:
        return
    if stale:
        save_output(path, arrays)
    if not pending:
        return

    model = model_class(timeout)

    for start in tqdm(range(0, len(pending), batch_size), desc=f"{model_key}/{language}", unit="batch"):
        batch = pending[start : start + batch_size]
        try:
            vectors = model.embed(
                [item["textified"] for item in batch],
                [item["role"] for item in batch],
            )
        except Exception as error:  # noqa: BLE001 - failed batches can be retried later.
            print(f"Skipped batch {start}:{start + len(batch)}: {type(error).__name__}: {error}")
            continue

        append_batch(arrays, batch, vectors)
        save_output(path, arrays)


def main() -> None:
    """Run resumable embedding calculation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=sorted(MODEL_CLASSES), action="append")
    parser.add_argument("--language", action="append")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")

    languages = args.language or sorted(path.stem.rsplit("_", 1)[-1] for path in TEXTIFIED_DIR.glob(DATASET_PATTERN))
    languages = list(dict.fromkeys(languages))
    models = args.model or [DEFAULT_MODEL]
    for language in languages:
        dataset_path = TEXTIFIED_DIR / f"pairwise_evaluation_dataset_textified_{language}.csv"
        if not dataset_path.exists():
            parser.error(f"No textified dataset found for language {language}: {dataset_path}")
    for model_key in models:
        for language in languages:
            process(model_key, language, args.batch_size, args.timeout, args.dry_run)


if __name__ == "__main__":
    main()
