"""Embedding model implementations available to the evaluation script."""

from .base import EmbeddingModel
from .bge_m3 import BGEM3
from .f2llm_330m import F2LLM330M
from .f2llm_600m import F2LLM600M
from .jina_embeddings_v3 import JinaEmbeddingsV3
from .mdenseon import MDenseOn
from .nomic_v2_moe import NomicV2MoE
from .qwen3_06b import Qwen306B

MODEL_CLASSES: dict[str, type[EmbeddingModel]] = {
    model.key: model for model in (BGEM3, F2LLM330M, F2LLM600M, JinaEmbeddingsV3, MDenseOn, NomicV2MoE, Qwen306B)
}

__all__ = ["EmbeddingModel", "MODEL_CLASSES"]
