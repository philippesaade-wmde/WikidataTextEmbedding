"""Shared interface for embedding backends used by the evaluation script."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar, Literal, get_args

import numpy as np
import torch
from huggingface_hub import snapshot_download

DEFAULT_MODEL_DIR = Path(__file__).resolve().parents[1] / "models"

Role = Literal[
    "document",
    "question",
    "entity_linking",
    "disambiguation",
    "property_linking",
]
VALID_ROLES = frozenset(get_args(Role))


class EmbeddingModel(ABC):
    """Base class implemented by every embedding model used in evaluation."""

    key: ClassVar[str]
    output_folder: ClassVar[str]

    def __init__(self, timeout: int):
        """Store the HTTP request timeout."""
        self.timeout = timeout

    @classmethod
    def download_model_snapshot(
        cls,
        repository: str,
        *,
        model_dir_env: str,
        timeout: int,
    ) -> str:
        """Download a model snapshot into its configured local directory."""
        model_root = Path(os.environ.get(model_dir_env, DEFAULT_MODEL_DIR)).expanduser()
        return snapshot_download(
            repo_id=repository,
            local_dir=model_root / cls.output_folder,
            token=os.environ.get("HF_TOKEN") or None,
            etag_timeout=timeout,
        )

    @staticmethod
    def select_device() -> torch.device:
        """Select the best available local device automatically."""
        if torch.cuda.is_available():
            return torch.device("cuda")
        if torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Return the text sent to the model for a query or passage."""
        return text

    @staticmethod
    def validate_role(role: str) -> None:
        """Raise an error when an input role is not supported."""
        if role not in VALID_ROLES:
            raise ValueError(f"Unsupported embedding role: {role!r}")

    @abstractmethod
    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Return one embedding vector for each input text."""

    @staticmethod
    @abstractmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Return the model-specific similarity for each candidate embedding."""
