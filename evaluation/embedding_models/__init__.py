"""Embedding model implementations available to the evaluation script."""

from .arctic_v2 import ArcticV2
from .base import EmbeddingModel
from .bekko_v1_a25m import BekkoV1A25M
from .bge_m3 import BGEM3
from .bidirlm_06b import BidirLM06B
from .e5_instruct import E5Instruct
from .f2llm_600m import F2LLM600M
from .granite_311m_r2 import Granite311MR2
from .gte_base import GTEBase
from .jina_embeddings_v3 import JinaEmbeddingsV3
from .mdenseon import MDenseOn
from .mmbert_32k_2d import MMBERT32K2D
from .nomic_v2_moe import NomicV2MoE
from .qwen3_06b import Qwen306B

MODEL_CLASSES: dict[str, type[EmbeddingModel]] = {
    model.key: model
    for model in (
        ArcticV2,
        BekkoV1A25M,
        BGEM3,
        BidirLM06B,
        E5Instruct,
        F2LLM600M,
        Granite311MR2,
        GTEBase,
        JinaEmbeddingsV3,
        MDenseOn,
        MMBERT32K2D,
        NomicV2MoE,
        Qwen306B,
    )
}

__all__ = ["EmbeddingModel", "MODEL_CLASSES"]
