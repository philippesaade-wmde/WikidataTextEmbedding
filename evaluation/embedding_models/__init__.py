"""Embedding model implementations available to the evaluation script."""

from .base import EmbeddingModel
from .bge_m3 import BGEM3
from .e5_instruct import E5Instruct
from .f2llm_330m import F2LLM330M
from .f2llm_600m import F2LLM600M
from .jina_embeddings_v3 import JinaEmbeddingsV3
from .mdenseon import MDenseOn
from .mmbert_32k_2d import MMBERT32K2D
from .nomic_v2_moe import NomicV2MoE
from .qwen3_06b import Qwen306B

MODEL_CLASSES: dict[str, type[EmbeddingModel]] = {
    model.key: model
    for model in (
        BGEM3,
        E5Instruct,
        F2LLM330M,
        F2LLM600M,
        JinaEmbeddingsV3,
        MDenseOn,
        MMBERT32K2D,
        NomicV2MoE,
        Qwen306B,
    )
}

__all__ = ["EmbeddingModel", "MODEL_CLASSES"]
