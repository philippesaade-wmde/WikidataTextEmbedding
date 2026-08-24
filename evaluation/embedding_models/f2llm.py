"""Shared local Transformers implementation for the F2LLM models."""

from __future__ import annotations

import numpy as np
import torch
import torch.nn.functional as functional
from transformers import AutoModel, AutoTokenizer

from .base import EmbeddingModel, Role


class F2LLMModel(EmbeddingModel):
    """Common local F2LLM behavior with use-case-specific query prompts."""

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
        dtype = self._dtype_for_device(self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
        # The model card's EOS pooling assumes right padding.
        self.tokenizer.padding_side = "right"
        self.model = AutoModel.from_pretrained(
            model_path,
            local_files_only=True,
            torch_dtype=dtype,
        )
        self.model.to(self.device)
        self.model.eval()

    @classmethod
    def prepare_text(cls, role: str, text: str) -> str:
        """Apply the role-specific F2LLM instruction to queries."""
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
        """Calculate F2LLM similarity with a dot product."""
        return candidate_embeddings @ query_embedding

    def embed(self, texts: list[str], roles: list[Role]) -> np.ndarray:
        """Embed a batch with the locally loaded model."""
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
            eos_positions = tokenized["attention_mask"].sum(dim=1) - 1
            batch_indices = torch.arange(len(texts), device=self.device)
            embeddings = hidden_states[batch_indices, eos_positions]
            embeddings = functional.normalize(embeddings, p=2, dim=1)
        return embeddings.float().cpu().numpy()

    @staticmethod
    def _dtype_for_device(device: torch.device) -> torch.dtype:
        """Choose a dtype supported efficiently by the selected device."""
        if device.type == "cuda":
            return torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16
        if device.type == "mps":
            return torch.float16
        return torch.float32
