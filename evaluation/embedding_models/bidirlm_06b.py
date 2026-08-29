"""Local Transformers adapter for BidirLM 0.6B Embedding."""

from __future__ import annotations

import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class BidirLM06B(EmbeddingModel):
    """Embed text with BidirLM/BidirLM-0.6B-Embedding."""

    key = "bidirlm-0.6b"
    output_folder = "bidirlm-0.6b"
    repository = "BidirLM/BidirLM-0.6B-Embedding"

    def __init__(self):
        """Download the model if needed and load it on the best local device."""
        model_path = self.download_model_snapshot()

        self.device = self.select_device()
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModel.from_pretrained(
            model_path,
            trust_remote_code=True,
            torch_dtype=torch.float32,
        )
        self.model.to(self.device)
        self.model.eval()

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate cosine similarity as specified by BidirLM."""
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
        """Embed a batch using attention-mask mean pooling."""
        self.validate_batch(texts, roles)
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
            hidden_states = self.model(**tokenized).last_hidden_state
            attention_mask = tokenized["attention_mask"].unsqueeze(-1).to(hidden_states.dtype)
            embeddings = (hidden_states * attention_mask).sum(dim=1) / attention_mask.sum(dim=1).clamp(min=1e-9)
        return embeddings.float().cpu().numpy()
