import time
import json
from src.wikidataCache import create_cache_embedding_db

class AstraDBConnect:
    def __init__(
            self, datastax_token, collection_name, model='jina', 
            batch_size=8, cache_embeddings=None):
        """
        Initialize the AstraDBConnect object with the corresponding embedding model.

        Parameters:
        - datastax_token (dict): Credentials for DataStax Astra, including token and API endpoint.
        - collection_name (str): Name of the collection (table) where data is stored.
        - model (str): The embedding model to use. Default is 'jina'.
        - batch_size (int): Number of documents to accumulate before pushing to AstraDB. Default is 8.
        - cache_embeddings (str): Name of the cache table.
        """
        from langchain_astradb import AstraDBVectorStore
        from astrapy.info import CollectionVectorServiceOptions
        from astrapy import DataAPIClient
        from astrapy.exceptions import InsertManyException
        from multiprocessing import Queue

        from transformers import AutoTokenizer
        from src.JinaAI import JinaAIEmbedder, JinaAIAPIEmbedder

        ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
        ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]
        ASTRA_DB_KEYSPACE = datastax_token["ASTRA_DB_KEYSPACE"]

        self.batch_size = batch_size
        self.model = model
        self.collection_name = collection_name
        self.doc_batch = Queue()
        self.InsertManyException = InsertManyException

        self.cache_on = (cache_embeddings is not None)
        if self.cache_on:
            self.cache_model = create_cache_embedding_db(
                table_name=cache_embeddings
            )

        client = DataAPIClient(datastax_token['ASTRA_DB_APPLICATION_TOKEN'])
        database0 = client.get_database(datastax_token['ASTRA_DB_API_ENDPOINT'])
        self.graph_store = database0.get_collection(collection_name)

        if model == 'nvidia':
            self.tokenizer = AutoTokenizer.from_pretrained('intfloat/e5-large-unsupervised', trust_remote_code=True, clean_up_tokenization_spaces=False)
            self.max_token_size = 500

            collection_vector_service_options = CollectionVectorServiceOptions(
                provider="nvidia",
                model_name="NV-Embed-QA"
            )

            self.vector_search = AstraDBVectorStore(
                collection_name=collection_name,
                collection_vector_service_options=collection_vector_service_options,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        elif model == 'jina':
            self.embeddings = JinaAIEmbedder(embedding_dim=1024)
            self.tokenizer = self.embeddings.tokenizer
            self.max_token_size = 1024

            self.vector_search = AstraDBVectorStore(
                collection_name=collection_name,
                embedding=self.embeddings,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )

        elif model == 'jinaapi':
            self.embeddings = JinaAIAPIEmbedder(embedding_dim=1024)
            self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)
            self.max_token_size = 1024

            self.vector_search = AstraDBVectorStore(
                collection_name=collection_name,
                embedding=self.embeddings,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        else:
            raise "Invalid model"

    def add_document(self, id, text, metadata):
        """
        Add a single document to the internal batch for future storage.

        Parameters:
        - id (str): The unique identifier for the document (e.g., a QID).
        - text (str): The text content of the document.
        - metadata (dict): Additional metadata about the document.
        """
        doc = {
            '_id': id,
            'content':text,
            'metadata':metadata
        }
        self.doc_batch.put(doc)

        # If we reach the batch size, push the accumulated documents to AstraDB
        if self.doc_batch.qsize() >= self.batch_size:
            self.push_batch()

    def push_batch(self):
        """
        Push the current batch of documents to AstraDB for storage.

        Caches the embeddings into a SQLite database.
        """
        if self.doc_batch.empty():
            return False

        docs = []
        for _ in range(self.batch_size):
            try:
                doc = self.doc_batch.get_nowait()
                cache = self._get_cached_embedding(doc['_id'])
                if cache is None:
                    docs.append(doc)
            except:
                # Queue is empty
                break

        if len(docs) == 0:
            return False

        while True:
            try:
                vectors = self.embeddings.embed_documents(
                    [doc['content'] for doc in docs]
                )
                break
            except Exception as e:
                print(e)
                time.sleep(3)

        while True:
            try:
                self.graph_store.insert_many(docs, vectors=vectors)
                break
            except self.InsertManyException as e:
                break
            except Exception as e:
                print(e)
                time.sleep(3)

        self.cache_model.add_bulk_cache([{
            'id': docs[i]['_id'],
            'embedding': vectors[i]}
            for i in range(len(docs))])

        return True

    def push_all(self):
        while True:
            if not self.push_batch():  # Stop when batch is empty
                break

    def get_similar_qids(self, query, filter={}, K=50):
        """
        Retrieve similar QIDs for a given query string.

        Parameters:
        - query (str): The text query used to find similar documents.
        - filter (dict): Additional filtering criteria. Default is an empty dict.
        - K (int): Number of top results to return. Default is 50.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
          where list_of_qids are the QIDs of the results and
          list_of_scores are the corresponding similarity scores.
        """
        results = self.vector_search.similarity_search_with_relevance_scores(query, k=K, filter=filter)
        qid_results = [r[0].metadata['QID']+"_"+r[0].metadata.get('Language', '') for r in results]
        score_results = [r[1] for r in results]
        return qid_results, score_results

    def batch_retrieve_comparative(self, queries_batch, comparative_batch, K=50, Language=None):
        """
        Retrieve similar documents in a comparative fashion for each query and comparative item.

        Parameters:
        - queries_batch (pd.Series or list): Batch of query texts.
        - comparative_batch (pd.DataFrame): A dataframe where each column represents a comparative group.
        - K (int): Number of top results to return for each query. Default is 50.
        - Language (str or None): Optional language filter. Default is None. Only supports one language.

        Returns:
        - tuple: (list_of_qids, list_of_scores), each a list for each query.
        """
        qids = [[] for _ in range(len(queries_batch))]
        scores = [[] for _ in range(len(queries_batch))]

        for comp_col in comparative_batch.columns:
            for i in range(len(queries_batch)):
                filter = {'QID': comparative_batch[comp_col].iloc[i]}
                if (Language is not None) and (Language != ""):
                        filter['Language'] = Language

                result = self.get_similar_qids(queries_batch.iloc[i], filter=filter, K=K)
                qids[i] = qids[i] + result[0]
                scores[i] = scores[i] + result[1]

        return qids, scores

    def batch_retrieve(self, queries_batch, K=50, Language=None):
        """
        Retrieve similar documents for a batch of queries, with optional language filtering.

        Parameters:
        - queries_batch (pd.Series or list): Batch of query texts.
        - K (int): Number of top results to return. Default is 50.
        - Language (str or None): Comma-separated list of language codes or None.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
        """
        filter = {}
        if (Language is not None) and (Language != ""):
            # Filter with an OR condition across multiple languages
            filter = {"$or": [{'Language': l} for l in Language.split(',')]}

        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K, filter=filter)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)

    def _cache_embedding(self, id, embedding):
        """
        Caches the text and its embedding in the SQLite database.

        Parameters:
        - text (str): The text string.
        - embedding (List[float]): The embedding vector for the text.
        """
        if self.cache_on:
            embedding = embedding.tolist()
            self.cache_model.add_cache(id=id, embedding=embedding)

    def _get_cached_embedding(self, id):
        """
        Retrieves a previously cached embedding for the specified text.

        Parameters:
        - text (str): The text string.

        Returns:
        - List[float] or None: The embedding if found in cache, otherwise None.
        """
        if self.cache_on:
            return self.cache_model.get_cache(id=id)
        return None

class KeywordSearchConnect:
    def __init__(self, url, index_name = 'wikidata'):
        """
        Initialize the WikidataKeywordSearch object with an Elasticsearch instance.

        Parameters:
        - url (str): URL (host) of the Elasticsearch server.
        - index_name (str): Name of the Elasticsearch index. Default is 'wikidata'.
        """
        from elasticsearch import Elasticsearch

        self.index_name = index_name
        self.es = Elasticsearch(url)
        self.create_index()

    def create_index(self):
        """
        Create the index with appropriate settings and mappings to optimize search.
        """
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body={
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "rebuilt_standard": {
                                "tokenizer": "standard",
                                "filter": ["lowercase", "stop"]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "text": {
                            "type": "text",
                            "analyzer": "default"
                        },
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "QID": {"type": "keyword"},
                                "Language": {"type": "keyword"},
                                "Date": {"type": "keyword"}
                            }
                        }
                    }
                }
            })

    def add_document(self, id, text, metadata):
        """
        Add a document to the Elasticsearch index.
        """
        doc = {
            'text': text,
            'metadata': {'QID': metadata['QID'], 'Language': metadata['Language']}
        }
        try:
            if self.es.exists(index=self.index_name, id=id):
                return
            self.es.index(index=self.index_name, id=id, body=doc)
        except ConnectionError as e:
            print("Connection error:", e)
            time.sleep(1)

    def push_batch(self):
        pass

    def search(self, query, K=50):
        """
        Perform a text search using Elasticsearch.
        """
        search_body = {
            "query": {
                "match": {
                    "text": query
                }
            },
            "size": K
        }
        try:
            response = self.es.search(index=self.index_name, body=search_body)
            return [hit for hit in response['hits']['hits']]
        except ConnectionError as e:
            print("Connection error:", e)
            return []

    def get_similar_qids(self, query, filter=[], K=50):
        """
        Retrieve documents based on similarity to a query, potentially with filtering.
        """
        search_body = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "text": query
                        }
                    },
                    "filter": filter
                }
            },
            "size": K
        }
        try:
            response = self.es.search(index=self.index_name, body=search_body)
            qid_results = [hit['_source']['metadata']['QID'] for hit in response['hits']['hits']]
            score_results = [hit['_score'] for hit in response['hits']['hits']]
            return qid_results, score_results

        except Exception as e:
            print("Search failed:", e)
            return []

    def batch_retrieve_comparative(self, queries_batch, comparative_batch, K=50, Language=None):
        """
        Retrieve similar documents in a comparative fashion for each query and comparative item.
        """
        qids = [[] for _ in range(len(queries_batch))]
        scores = [[] for _ in range(len(queries_batch))]

        for i, query in enumerate(queries_batch):
            for comp_col in comparative_batch.columns:
                filter = []
                # Apply language filter if specified
                if Language:
                    filter.append({"term": {"metadata.Language": Language}})
                # Apply QID filter specific to the comparative group
                qid_filter = comparative_batch[comp_col].iloc[i]
                filter.append({"term": {"metadata.QID": qid_filter}})

                result_qids, result_scores = self.get_similar_qids(query, filter=filter, K=K)
                qids[i].extend(result_qids)
                scores[i].extend(result_scores)

        return qids, scores

    def batch_retrieve(self, queries_batch, K=50, Language=None):
        """
        Perform batch searches and handle potential connection issues.
        """
        filter = []
        if Language:
            languages = Language.split(',')
            filter.append({"bool": {"should": [{"term": {"metadata.Language": lang}} for lang in languages]}})

        results = []
        for query in queries_batch:
            try:
                result = self.get_similar_qids(query, K=K, filter=filter)
                results.append(result)
            except ConnectionError as e:
                print("Connection error during batch processing:", e)
                time.sleep(1)

        qids, scores = zip(*results) if results else ([], [])
        return list(qids), list(scores)