"""Local Transformers adapter for E5 Instruct."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class E5Instruct(EmbeddingModel):
    """Embed queries and documents with E5 Instruct."""

    key = "e5-instruct"
    output_folder = "e5-instruct"
    repository = "intfloat/multilingual-e5-large-instruct"
    query_instructions: dict[str, str] = {
        "question": "Given a question, retrieve passages containing the information needed to answer it.",
        "entity_linking": "Given a sentence, retrieve passages about the entity mentioned in it.",
        "disambiguation": "Given a sentence with an ambiguous mention, retrieve passages about the intended entity.",
        "property_linking": "Given a question, retrieve passages about the Wikidata property needed to answer it.",
    }

    def __init__(self):
        """Download the model if needed and load it on the best local device."""
        model_path = self.download_model_snapshot()

        self.device = self.select_device()
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)
        self.model = AutoModel.from_pretrained(
            model_path,
            dtype=torch.float32,
        )
        self.model.to(self.device)
        self.model.eval()

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Apply a role-specific instruction to queries only."""
        cls.validate_role(role)
        if role == "document":
            return text
        instruction = cls.query_instructions[role]
        return f"Instruct: {instruction}\nQuery: {text}"

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate similarity between normalized embeddings with a dot product."""
        return candidate_embeddings @ query_embedding

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch using normalized attention-mask average pooling."""
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
            embeddings = functional.normalize(embeddings, p=2, dim=1)
        return embeddings.float().cpu().numpy()
