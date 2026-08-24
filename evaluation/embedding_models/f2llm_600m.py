"""Local Transformers adapter for F2LLM 600M."""

from __future__ import annotations

from .f2llm import F2LLMModel


class F2LLM600M(F2LLMModel):
    """Embed text with codefuse-ai/F2LLM-v2-0.6B."""

    key = "f2llm-600m"
    output_folder = "F2LLM-v2-600m"
    repository = "codefuse-ai/F2LLM-v2-0.6B"
