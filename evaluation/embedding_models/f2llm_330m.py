"""Local Transformers adapter for F2LLM 330M."""

from __future__ import annotations

from .f2llm import F2LLMModel


class F2LLM330M(F2LLMModel):
    """Embed text with codefuse-ai/F2LLM-v2-330M."""

    key = "f2llm-330m"
    output_folder = "F2LLM-v2-330m"
    repository = "codefuse-ai/F2LLM-v2-330M"
