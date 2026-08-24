"""Embedding model implementations available to the evaluation script."""

from .base import EmbeddingModel
from .f2llm_330m import F2LLM330M
from .f2llm_600m import F2LLM600M
from .jina_embeddings_v3 import JinaEmbeddingsV3
from .mdenseon import MDenseOn

MODEL_CLASSES: dict[str, type[EmbeddingModel]] = {
    model.key: model for model in (F2LLM330M, F2LLM600M, JinaEmbeddingsV3, MDenseOn)
}

__all__ = ["EmbeddingModel", "MODEL_CLASSES"]
