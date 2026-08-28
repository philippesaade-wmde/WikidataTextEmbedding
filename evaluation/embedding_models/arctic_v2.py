"""Local Transformers adapter for Snowflake Arctic v2."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class ArcticV2(EmbeddingModel):
    """Embed queries and documents with Snowflake Arctic v2."""

    key = "arctic-v2"
    output_folder = "arctic-v2"
    repository = "Snowflake/snowflake-arctic-embed-l-v2.0"

    def __init__(self):
        """Download the model if needed and load it on the best local device."""
        model_path = self.download_model_snapshot()

        self.device = self.select_device()
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModel.from_pretrained(
            model_path,
            add_pooling_layer=False,
            dtype=torch.float32,
        )
        self.model.to(self.device)
        self.model.eval()

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Add Arctic's retrieval prefix to queries only."""
        cls.validate_role(role)
        return text if role == "document" else f"query: {text}"

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate similarity between normalized embeddings with a dot product."""
        return candidate_embeddings @ query_embedding

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch using normalized CLS pooling."""
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
