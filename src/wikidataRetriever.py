from langchain_astradb import AstraDBVectorStore
from langchain_core.documents import Document
from astrapy.info import CollectionVectorServiceOptions
from transformers import AutoTokenizer
import requests
from JinaAI import JinaAIEmbedder
import time
from elasticsearch import Elasticsearch

from mediawikiapi import MediaWikiAPI
from mediawikiapi.config import Config

class AstraDBConnect:
    def __init__(self, datastax_token, collection_name, model='nvidia', batch_size=8, cache_embeddings=False):
        ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
        ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]
        ASTRA_DB_KEYSPACE = datastax_token["ASTRA_DB_KEYSPACE"]

        self.batch_size = batch_size
        self.model = model
        self.collection_name = collection_name
        self.doc_batch = []
        self.id_batch = []

        if model == 'nvidia':
            self.tokenizer = AutoTokenizer.from_pretrained('intfloat/e5-large-unsupervised', trust_remote_code=True, clean_up_tokenization_spaces=False)
            self.max_token_size = 500

            collection_vector_service_options = CollectionVectorServiceOptions(
                provider="nvidia",
                model_name="NV-Embed-QA"
            )

            self.graph_store = AstraDBVectorStore(
                collection_name=collection_name,
                collection_vector_service_options=collection_vector_service_options,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        elif model == 'jina':
            embeddings = JinaAIEmbedder(embedding_dim=1024, cache=cache_embeddings)
            self.tokenizer = embeddings.tokenizer
            self.max_token_size = 1024

            self.graph_store = AstraDBVectorStore(
                collection_name=collection_name,
                embedding=embeddings,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        else:
            raise "Invalid model"

    def add_document(self, id, text, metadata):
        doc = Document(page_content=text, metadata=metadata)
        self.doc_batch.append(doc)
        self.id_batch.append(id)

        if len(self.doc_batch) >= self.batch_size:
            self.push_batch()

    def push_batch(self):
        while True:
            try:
                self.graph_store.add_documents(self.doc_batch, ids=self.id_batch)
                self.doc_batch = []
                self.id_batch = []
                break
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        print("Waiting for internet connection...")

    def get_similar_qids(self, query, filter={}, K=50):
        while True:
            try:
                results = self.graph_store.similarity_search_with_relevance_scores(query, k=K, filter=filter)
                qid_results = [r[0].metadata['QID'] for r in results]
                score_results = [r[1] for r in results]
                return qid_results, score_results
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(5)

    def batch_retrieve_comparative(self, queries_batch, comparative_batch, K=50, Language=None):
        qids = [[] for _ in range(len(queries_batch))]
        scores = [[] for _ in range(len(queries_batch))]

        for comp_col in comparative_batch.columns:
            filter = {'QID': comparative_batch[comp_col].iloc[i]}
            if (Language is not None) and (Language != ""):
                    filter['Language'] = Language

            results = [
                self.get_similar_qids(queries_batch.iloc[i], filter=filter, K=K)
                for i in range(len(queries_batch))
            ]

            for i, (temp_qid, temp_score) in enumerate(results):
                qids[i] = qids[i] + temp_qid
                scores[i] = scores[i] + temp_score
        return qids, scores

    def batch_retrieve(self, queries_batch, K=50, Language=None):
        filter = {}
        if (Language is not None) and (Language != ""):
            filter = {"$or": [{'Language': l} for l in Language.split(',')]}

        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K, filter=filter)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)

class WikidataCirrusSeach:
    def __init__(self, id_filter=[]):
        WIKIDATA_USER_AGENT = "langchain-wikidata"
        WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"

        self.wikidata_mw = MediaWikiAPI(Config(user_agent=WIKIDATA_USER_AGENT, mediawiki_url=WIKIDATA_API_URL))
        self.id_filter = id_filter

    def search(self, query, K=50):
        query = ' OR '.join(query.split())

        start_n = 0
        end_n = 1000
        results = []
        while True:
            resp = self.wikidata_mw.search(query, results=end_n)
            for r in resp[start_n:]:
                if (len(self.id_filter) == 0) or (r in self.id_filter):
                    results.append(r)

                if len(results) >= K:
                    break

            if (len(results) >= K) or (len(resp) < end_n) or (self.end_n >= 10000):
                break

            start_n += 1000
            end_n += 1000

        return results

    def get_similar_qids(self, query, filter_qid={}, K=50):
        while True:
            try:
                results = self.search(query, K=K)
                qid_results = results
                score_results = [1]*len(results)
                return qid_results, score_results
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(5)

    def batch_retrieve(self, queries_batch, K=50):
        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)

class WikidataKeywordSearch:
    def __init__(self, url, index_name = 'wikidata'):
        self.index_name = index_name
        self.es = Elasticsearch(url)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body={
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text"
                    }
                }
            }
        })

    def search(self, query, K=50):
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "text": {
                                    "query": query,
                                    "operator": "or",
                                    "boost": 1.0
                                }
                            }
                        },
                        {
                            "match_all": {
                                "boost": 0.01  # Lower boost to make match_all results less relevant
                            }
                        }
                    ]
                }
            },
            "size": K,
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ]
        }
        response = self.es.search(index=self.index_name, body=search_body)
        return [hit for hit in response['hits']['hits']]

    def get_similar_qids(self, query, filter_qid={}, K=50):
        results = self.search(query, K=K)
        qid_results = [r['_id'].split("_")[0] for r in results]
        score_results = [r['_score'] for r in results]
        return qid_results, score_results

    def batch_retrieve(self, queries_batch, K=50):
        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)