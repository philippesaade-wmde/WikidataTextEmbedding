import json
import requests
import numpy as np
import base64

# import torch  # torch no long imported


from typing import List
from src.wikidataCache import create_cache_embedding_db


class JinaAIEmbedder:
    def __init__(
            self, passage_task="retrieval.passage",
            query_task="retrieval.query", embedding_dim=1024, cache=None):
        """
        Initializes the JinaAIEmbedder class with the model, tokenizer,
        and task identifiers.

        Parameters:
        - passage_task (str): Task identifier for embedding documents.
            Defaults to "retrieval.passage".
        - query_task (str): Task identifier for embedding queries.
            Defaults to "retrieval.query".
        - embedding_dim (int): Dimensionality of the embeddings.
            Defaults to 1024.
        - cache (str): Name of caching table.
        - api_key_path (str): Path to the JSON file containing the
            Jina API key. Defaults to "../API_tokens/jina_api.json".
        """
        from transformers import AutoModel, AutoTokenizer

        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

        self.model = AutoModel.from_pretrained(
            "jinaai/jina-embeddings-v3",
            trust_remote_code=True
        ).to('cuda')
        self.tokenizer = AutoTokenizer.from_pretrained(
            "jinaai/jina-embeddings-v3",
            trust_remote_code=True
        )

        self.cache = (cache is not None)
        if self.cache:
            self.cache_model = create_cache_embedding_db(table_name=cache)

    def _cache_embedding(self, text: str, embedding: List[float]):
        """
        Caches the text and its embedding in the SQLite database.

        Parameters:
        - text (str): The text string.
        - embedding (List[float]): The embedding vector for the text.
        """
        if self.cache:
            embedding = embedding.tolist()
            self.cache_model.add_cache(id=text, embedding=embedding)

    def _get_cached_embedding(self, text: str) -> List[float]:
        """
        Retrieves a previously cached embedding for the specified text.

        Parameters:
        - text (str): The text string.

        Returns:
        - List[float] or None: The embedding if found in cache, otherwise None.
        """
        if self.cache:
            return self.cache_model.get_cache(id=text)
        return None

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of document (passage) texts.

        Caching is not used here by default to avoid storing
        large numbers of document embeddings.

        Parameters:
        - texts (List[str]): A list of document texts to embed.

        Returns:
        - List[List[float]]: A list of embedding vectors, each corresponding
        to a document.
        """

        with torch.no_grad():
            embeddings = self.model.encode(
                texts,
                task=self.passage_task,
                truncate_dim=self.embedding_dim
            )

        return embeddings

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query string, optionally using
        and updating the cache.

        Parameters:
        - text (str): The query text to embed.

        Returns:
        - List[float]: The embedding vector corresponding to the query.
        """
        cached_embedding = self._get_cached_embedding(text)
        if cached_embedding:
            return cached_embedding

        with torch.no_grad():
            embedding = self.model.encode(
                [text],
                task=self.query_task,
                truncate_dim=self.embedding_dim
            )[0]

            self._cache_embedding(text, embedding)
            return embedding


class JinaAIAPIEmbedder:
    def __init__(
            self, passage_task="retrieval.passage",
            query_task="retrieval.query", embedding_dim=1024,
            api_key_path="../API_tokens/jina_api.json"):  # cache=False,
        """
        Initializes the JinaAIEmbedder class with the model, tokenizer,
        and task identifiers.

        Parameters:
        - passage_task (str): Task identifier for embedding documents.
            Defaults to "retrieval.passage".
        - query_task (str): Task identifier for embedding queries.
            Defaults to "retrieval.query".
        - embedding_dim (int): Dimensionality of the embeddings.
            Defaults to 1024.
        - cache (str): Name of caching table.  # BUG: cache is unused
        - api_key_path (str): Path to the JSON file containing
            the Jina API key. Defaults to "../API_tokens/jina_api.json".
        """
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

        self.api_key = json.load(open(api_key_path, 'r+'))['API_KEY']

    def api_embed(self, texts, task="retrieval.query"):
        """
        Generates an embedding for the given text using the Jina Embeddings API

        Parameters:
        - text (str): The text to embed.
        - task (str): The task identifier
            (e.g., "retrieval.query" or "retrieval.passage").

        Returns:
        - np.ndarray: The resulting embedding vector as a NumPy array.
        """
        url = 'https://api.jina.ai/v1/embeddings'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }

        if type(texts) is str:
            texts = [texts]

        data = {
            "model": "jina-embeddings-v3",
            "dimensions": self.embedding_dim,
            "embedding_type": "base64",
            "task": task,
            "late_chunking": False,
            "input": texts
        }

        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Ensure request was successful
        response_data = response.json()

        embeddings = []
        for item in response_data['data']:
            binary_data = base64.b64decode(item['embedding'])
            # Ensure float32 format for compatibility across models
            embedding_array = np.frombuffer(binary_data, dtype='<f4')
            embeddings.append(embedding_array.tolist())

        return embeddings

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of document (passage) texts.

        Caching is not used here by default to avoid storing large numbers
        of document embeddings.

        Parameters:
        - texts (List[str]): A list of document texts to embed.

        Returns:
        - List[List[float]]: A list of embedding vectors, each corresponding
        to a document.
        """
        embeddings = self.api_embed(texts, task=self.passage_task)
        return embeddings

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query string, optionally using
        and updating the cache.

        Parameters:
        - text (str): The query text to embed.

        Returns:
        - List[float]: The embedding vector corresponding to the query.
        """
        embedding = self.api_embed([text], task=self.query_task)
        return embedding


class JinaAIReranker:
    def __init__(self, max_tokens=1024):
        """
        Initializes the JinaAIReranker with a maximum token length
        and the Jina Reranker model.

        Parameters:
        - max_tokens (int): Maximum sequence length for the reranker
        (must be <= 1024).

        Raises:
        - ValueError: If max_tokens is greater than 1024.
        """
        from transformers import AutoModelForSequenceClassification

        if max_tokens > 1024:
            raise ValueError("Max token should be less than or equal to 1024")

        self.max_tokens = max_tokens
        self.model = AutoModelForSequenceClassification.from_pretrained(
            'jinaai/jina-reranker-v2-base-multilingual',
            trust_remote_code=True
        ).to('cuda')

    def rank(self, query: str, texts: List[str]) -> List[float]:
        """
        Scores a list of documents based on their relevance to the given query.

        Parameters:
        - query (str): The user's query text.
        - texts (List[str]): A list of document texts to rank.

        Returns:
        - List[float]: A list of relevance scores, each corresponding
        to one document.
        """
        sentence_pairs = [[query, doc] for doc in texts]

        with torch.no_grad():
            return self.model.compute_score(
                sentence_pairs,
                max_length=self.max_tokens
            )
