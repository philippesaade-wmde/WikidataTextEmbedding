"""Jina API adapter for jina-embeddings-v3."""

from __future__ import annotations

import base64
import os

import numpy as np
import requests

from .base import EmbeddingModel, Role


class JinaEmbeddingsV3(EmbeddingModel):
    """Embed queries and passages through the Jina embeddings API."""

    key = "jina-v3"
    output_folder = "jina-embeddings-v3"
    model = "jina-embeddings-v3"
    dimensions = 512
    endpoint = "https://api.jina.ai/v1/embeddings"

    def __init__(self):
        """Initialize the authenticated Jina API session."""
        api_key = os.environ.get("JINA_API_KEY", "").strip()
        if not api_key:
            raise ValueError("Set JINA_API_KEY before embedding with Jina")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch, using Jina's query task for queries and passage task otherwise."""
        self.validate_batch(texts, roles)

        vectors: list[np.ndarray | None] = [None] * len(texts)
        for task in ("retrieval.query", "retrieval.passage"):
            indices = [index for index, role in enumerate(roles) if self._task_for_role(role) == task]
            if not indices:
                continue
            response = self.session.post(
                self.endpoint,
                json={
                    "model": self.model,
                    "dimensions": self.dimensions,
                    "embedding_type": "base64",
                    "task": task,
                    "late_chunking": False,
                    "input": [texts[index] for index in indices],
                },
            )
            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                raise RuntimeError(f"Jina API returned HTTP {response.status_code}: {response.text}") from error
            items = response.json().get("data", [])
            if len(items) != len(indices):
                raise ValueError(f"Jina returned {len(items)} embeddings for {len(indices)} inputs")
            for index, item in zip(indices, items, strict=True):
                binary = base64.b64decode(item["embedding"])
                vectors[index] = np.frombuffer(binary, dtype="<f4")

        if any(vector is None for vector in vectors):
            raise ValueError("Jina did not return an embedding for every input")
        return np.asarray(vectors, dtype=np.float32)

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate Jina similarity with a dot product."""
        return candidate_embeddings @ query_embedding

    @staticmethod
    def _task_for_role(role: str) -> str:
        return "retrieval.passage" if role == "document" else "retrieval.query"
