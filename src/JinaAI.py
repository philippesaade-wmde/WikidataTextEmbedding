"""Embed and rank text with Jina models and APIs."""

import base64

import numpy as np
import requests


class JinaAITokenizer:
    """Load and forward calls to the Jina embeddings tokenizer."""

    def __init__(self):
        """Load the pretrained tokenizer."""
        from transformers import AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)

    def __call__(self, *args, **kwargs):
        """Forward calls to the wrapped tokenizer."""
        return self.tokenizer(*args, **kwargs)


class JinaAIEmbedder:
    """Embed texts locally with the Jina embeddings model."""

    def __init__(
        self,
        passage_task="retrieval.passage",
        query_task="retrieval.query",
        embedding_dim=512,
        device="cuda",
    ):
        """Initialize the local Jina embedding model."""
        import torch
        from transformers import AutoModel

        self.torch = torch

        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True).to(device)

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed passage documents with the local model."""
        with self.torch.no_grad():
            embeddings = self.model.encode(texts, task=self.passage_task, truncate_dim=self.embedding_dim)

        return embeddings.tolist()

    def embed_query(self, text: str) -> list[float]:
        """Embed one query string with the local model."""
        with self.torch.no_grad():
            embedding = self.model.encode([text], task=self.query_task, truncate_dim=self.embedding_dim)[0]
            return embedding.tolist()

    def __call__(self, text: str, task: str) -> list[float]:
        """Embed text according to the requested Jina task name."""
        if task == self.query_task:
            return self.embed_query(text)
        elif task == self.passage_task:
            return self.embed_documents([text])[0]
        else:
            raise ValueError("Invalid task specified")


class JinaAIAPIEmbedder:
    """Embed texts through the Jina embeddings API."""

    def __init__(
        self,
        passage_task="retrieval.passage",
        query_task="retrieval.query",
        embedding_dim=512,
        api_key: str | None = None,
    ):
        """Initialize API credentials and embedding task names."""
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

        self.api_key = api_key

        if not self.api_key:
            raise ValueError("Jina API key not found.")

    def api_embed(self, texts, task="retrieval.query"):
        """Request embeddings from the Jina embeddings API."""
        url = "https://api.jina.ai/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        if type(texts) is str:
            texts = [texts]

        data = {
            "model": "jina-embeddings-v3",
            "dimensions": self.embedding_dim,
            "embedding_type": "base64",
            "task": task,
            "late_chunking": False,
            "input": texts,
        }

        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Ensure request was successful
        response_data = response.json()

        embeddings = []
        for item in response_data["data"]:
            binary_data = base64.b64decode(item["embedding"])
            # Ensure float32 format for compatibility across models
            embedding_array = np.frombuffer(binary_data, dtype="<f4")
            embeddings.append(embedding_array.tolist())

        return embeddings

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed passage documents through the Jina API."""
        embeddings = self.api_embed(texts, task=self.passage_task)
        return embeddings

    def embed_query(self, text: str) -> list[float]:
        """Embed one query string through the Jina API."""
        embeddings = self.api_embed([text], task=self.query_task)
        return embeddings[0]

    def __call__(self, text: str, task: str) -> list[float]:
        """Embed text through the Jina API using the provided task."""
        return self.api_embed([text], task=task)[0]


class JinaAIReranker:
    """Score query-document relevance with the Jina reranker."""

    def __init__(self, max_tokens=1024, device="cuda"):
        """Initialize the local Jina reranker model."""
        import torch
        from transformers import AutoModelForSequenceClassification

        self.torch = torch

        if max_tokens > 1024:
            raise ValueError("Max token should be less than or equal to 1024")

        self.max_tokens = max_tokens
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "jinaai/jina-reranker-v2-base-multilingual", trust_remote_code=True
        ).to(device)

    def rank(self, query: str, texts: list[str]) -> list[float]:
        """Score documents against a query."""
        sentence_pairs = [[query, doc] for doc in texts]

        with self.torch.no_grad():
            return self.model.compute_score(sentence_pairs, max_length=self.max_tokens)

    def __call__(self, query: str, texts: list[str]) -> list[float]:
        """Score documents against a query."""
        return self.rank(query, texts)
