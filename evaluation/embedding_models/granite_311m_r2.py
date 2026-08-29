"""Local Transformers adapter for Granite Embedding 311M Multilingual R2."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class Granite311MR2(EmbeddingModel):
    """Embed text with IBM Granite Embedding 311M Multilingual R2."""

    key = "granite-311m-r2"
    output_folder = "granite-311m-r2"
    repository = "ibm-granite/granite-embedding-311m-multilingual-r2"

    def __init__(self):
        """Download the model if needed and load it on the best local device."""
        model_path = self.download_model_snapshot()

        self.device = self.select_device()
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModel.from_pretrained(
            model_path,
            torch_dtype=torch.float32,
        )
        self.model.to(self.device)
        self.model.eval()

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate similarity between normalized embeddings with a dot product."""
        return candidate_embeddings @ query_embedding

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch using normalized CLS pooling at the native dimension."""
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
            embeddings = self.model(**tokenized).last_hidden_state[:, 0]
            embeddings = functional.normalize(embeddings, p=2, dim=1)
        return embeddings.float().cpu().numpy()
