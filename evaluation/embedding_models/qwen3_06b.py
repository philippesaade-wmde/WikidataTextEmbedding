"""Local Transformers adapter for Qwen3-0.6B."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class Qwen306B(EmbeddingModel):
    """Embed queries and documents with Qwen3-0.6B."""

    key = "qwen3-0.6b"
    output_folder = "Qwen3-0.6B"
    repository = "Qwen/Qwen3-Embedding-0.6B"
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
        self.tokenizer.padding_side = "left"
        self.model = AutoModel.from_pretrained(
            model_path,
            torch_dtype=self._dtype_for_device(self.device),
        )
        self.model.to(self.device)
        self.model.eval()

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Apply a role-specific retrieval instruction to queries only."""
        cls.validate_role(role)
        if role == "document":
            return text
        instruction = cls.query_instructions[role]
        return f"Instruct: {instruction}\nQuery:{text}"

    @staticmethod
    def calculate_similarity(
        query_embedding: np.ndarray,
        candidate_embeddings: np.ndarray,
    ) -> np.ndarray:
        """Calculate similarity between normalized embeddings with a dot product."""
        return candidate_embeddings @ query_embedding

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch using normalized last-token pooling at the native dimension."""
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
            embeddings = self._last_token_pool(hidden_states, tokenized["attention_mask"])
            embeddings = functional.normalize(embeddings, p=2, dim=1)
        return embeddings.float().cpu().numpy()

    @staticmethod
    def _last_token_pool(
        hidden_states: torch.Tensor,
        attention_mask: torch.Tensor,
    ) -> torch.Tensor:
        """Return the final non-padding token for left- or right-padded inputs."""
        left_padded = attention_mask[:, -1].sum() == attention_mask.shape[0]
        if left_padded:
            return hidden_states[:, -1]
        sequence_lengths = attention_mask.sum(dim=1) - 1
        batch_indices = torch.arange(hidden_states.shape[0], device=hidden_states.device)
        return hidden_states[batch_indices, sequence_lengths]

    @staticmethod
    def _dtype_for_device(device: torch.device) -> torch.dtype:
        """Choose a dtype supported efficiently by the selected device."""
        if device.type == "cuda":
            return torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
        if device.type == "mps":
            return torch.float16
        return torch.float32
