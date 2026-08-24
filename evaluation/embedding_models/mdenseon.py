"""Local Transformers adapter for LightOn mDenseOn."""

from __future__ import annotations

import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class MDenseOn(EmbeddingModel):
    """Embed queries and documents with lightonai/mDenseOn."""

    key = "mdenseon"
    output_folder = "mDenseOn"
    repository = "lightonai/mDenseOn"

    def __init__(self, timeout: int):
        """Download the model if needed and load it on the best local device."""
        super().__init__(timeout)
        model_path = self.download_model_snapshot(
            self.repository,
            model_dir_env="MDENSEON_MODEL_DIR",
            timeout=timeout,
        )

        self.device = self.select_device()
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            local_files_only=True,
        )
        self.model = AutoModel.from_pretrained(
            model_path,
            local_files_only=True,
            torch_dtype=torch.float32,
        )
        self.model.to(self.device)
        self.model.eval()

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Apply mDenseOn's retrieval prefix for the input role."""
        cls.validate_role(role)
        prefix = "document: " if role == "document" else "query: "
        return f"{prefix}{text}"

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate cosine similarity as specified by mDenseOn."""
        query = np.asarray(query_embedding, dtype=np.float32)
        candidates = np.asarray(candidate_embeddings, dtype=np.float32)
        if query.ndim != 1 or candidates.ndim != 2:
            raise ValueError("Expected one query vector and a matrix of candidate vectors")
        query_norm = np.linalg.norm(query)
        candidate_norms = np.linalg.norm(candidates, axis=1)
        if query_norm == 0 or np.any(candidate_norms == 0):
            raise ValueError("Cannot calculate cosine similarity for zero-length vectors")
        return (candidates @ query) / (candidate_norms * query_norm)

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed prepared texts locally using CLS pooling."""
        if len(texts) != len(roles):
            raise ValueError("texts and roles must have the same length")
        for role in roles:
            self.validate_role(role)
        if not texts:
            return np.empty((0, self.model.config.hidden_size), dtype=np.float32)

        tokenized = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )
        tokenized = {name: tensor.to(self.device) for name, tensor in tokenized.items()}
        with torch.inference_mode():
            embeddings = self.model(**tokenized).last_hidden_state[:, 0]
        return embeddings.float().cpu().numpy()
