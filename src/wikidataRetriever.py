import time
import traceback
import requests
import re

class AstraDBConnect:
    def __init__(
            self, datastax_token, collection_name, model='jina',
            batch_size=8):
        """
        Initialize the AstraDBConnect object with the corresponding embedding model.

        Parameters:
        - datastax_token (dict): Credentials for DataStax Astra, including token and API endpoint.
        - collection_name (str): Name of the collection (table) where data is stored.
        - model (str): The embedding model to use. Default is 'jina'.
        - batch_size (int): Number of documents to accumulate before pushing to AstraDB. Default is 8.
        """
        from astrapy import DataAPIClient
        from astrapy.exceptions import CollectionInsertManyException
        from astrapy.api_options import APIOptions, TimeoutOptions

        from multiprocessing import Queue

        from transformers import AutoTokenizer
        from src.JinaAI import JinaAIEmbedder, JinaAIAPIEmbedder

        ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
        ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]

        self.batch_size = batch_size
        self.model = model
        self.collection_name = collection_name
        self.duplicate_exception = CollectionInsertManyException
        self.doc_batch = Queue()

        timeout_options = TimeoutOptions(request_timeout_ms=1000000)
        api_options = APIOptions(timeout_options=timeout_options)

        client = DataAPIClient(
            ASTRA_DB_APPLICATION_TOKEN,
            api_options=api_options
        )
        database0 = client.get_database(ASTRA_DB_API_ENDPOINT)
        self.graph_store = database0.get_collection(collection_name)

        if model == 'jina':
            self.embeddings = JinaAIEmbedder(embedding_dim=1024)
            self.tokenizer = self.embeddings.tokenizer
            self.max_token_size = 1024

        elif model == 'jinaapi':
            self.embeddings = JinaAIAPIEmbedder(embedding_dim=1024)
            self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)
            self.max_token_size = 1024

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
        """
        if self.doc_batch.empty():
            return False

        docs = []
        for _ in range(self.batch_size):
            try:
                doc = self.doc_batch.get_nowait()
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
                traceback.print_exc()
                time.sleep(3)

        for i in range(len(docs)):
            docs[i]['$vector'] = vectors[i]

        while True:
            try:
                self.graph_store.insert_many(docs)
                break
            except self.duplicate_exception as e:
                break # Ignore duplicate IDs error. Non-duplicates in the list get pushed anyway
            except Exception as e:
                traceback.print_exc()
                time.sleep(3)

        return True

    def push_all(self):
        while True:
            if not self.push_batch():  # Stop when batch is empty
                break

    def get_similar_qids(self, query, filter={}, K=10):
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
        embedding = self.embeddings.embed_query(query)
        relevant_items = self.graph_store.find(
            filter,
            sort={"$vector": embedding},
            limit=K,
            include_similarity=True
        )

        qid_results = []
        score_results = []
        for item in relevant_items:

            ID = item['metadata'].get('ID')

            qid_results.append(ID + \
                "_" + item['metadata'].get('Language', '') + \
                "_" + str(item['metadata'].get('ChunkID', ''))
            )
            score_results.append(item['$similarity'])

        return qid_results, score_results

    def construct_filter(self,
                         languages=None,
                         ids=None,
                         isitem=False,
                         isproperty=False):
        filter = {}

        if (languages is not None) and (languages != ""):
            languages = languages.split(',')
            if len(languages) > 1:
                filter = {
                    "$or": [{'metadata.Language': l} for l in languages]
                }
            else:
                filter['metadata.Language']= languages[0]

        if (ids is not None) and (ids != ""):
            ids = ids.split(',')
            if len(ids) > 1:
                if '$or' in filter:
                    filter['$and'] = [
                        {'$or': [{'metadata.ID': l} for l in ids]},
                        {'$or': filter['$or']}
                    ]
                else:
                    filter['$or'] = [{'metadata.ID': l} for l in ids]
            else:
                filter['metadata.ID']= ids[0]

        if isitem:
            filter['metadata.IsItem']= True

        if isproperty:
            filter['metadata.IsProperty']= True

        return filter

    def batch_retrieve_comparative(self,
                                   queries_batch,
                                   comparative_batch,
                                   K=20,
                                   Language=None,
                                   getItems=True):
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

                filter = self.construct_filter(
                    langauges=Language,
                    ids=comparative_batch[comp_col].iloc[i],
                    isitem = getItems,
                    isproperty = not getItems
                )

                result = self.get_similar_qids(
                    queries_batch.iloc[i],
                    filter=filter,
                    K=K
                )
                qids[i] = qids[i] + result[0]
                scores[i] = scores[i] + result[1]

        return qids, scores

    def batch_retrieve(self,
                       queries_batch,
                       K=20,
                       Language=None,
                       getItems=True):
        """
        Retrieve similar documents for a batch of queries, with optional language filtering.

        Parameters:
        - queries_batch (pd.Series or list): Batch of query texts.
        - K (int): Number of top results to return. Default is 50.
        - Language (str or None): Comma-separated list of language codes or None.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
        """

        filter = self.construct_filter(
            languages = Language,
            isitem = getItems,
            isproperty = not getItems
        )

        results = [
            self.get_similar_qids(
                queries_batch.iloc[i],
                K=K,
                filter=filter
            )
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)

class KeywordSearchConnect:

    def clean_query(self, query):
        """
        Split the query into individual terms separated by "OR" for the search.

        Parameters:
        - query (str): The query string to process.

        Returns:
        - str: The cleaned query string suitable for searching.
        """
        # Remove stopwords
        query_terms = query.split()

        # Join terms with "OR" for Elasticsearch compatibility
        cleaned_query = " OR ".join(query_terms)
        if cleaned_query == "":
            return "None"
        return cleaned_query[:300] # Max allowed characters is 300

    def get_similar_qids(self, query, filter={}, K=100):
        """
        Retrieve similar QIDs for a given query string.

        Parameters:
        - query (str): The text query used to find similar documents.
        - K (int): Number of top results to return. Default is 50.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
          where list_of_qids are the QIDs of the results and
          list_of_scores are the corresponding similarity scores.
        """

        # Clean the query
        cleaned_query = self.clean_query(query)

        params = {
            'action': 'query',
            'list': 'search',
            'prop': '',
            'meta': '',
            'srinfo': '',
            'srprop': '',
            'srsort': 'relevance',
            'format': 'json',
            'formatversion': '2',
            'srsearch': cleaned_query,
            'srlimit': K,
        }

        # Make the API request to Wikidata CirrusSearch
        url = "https://www.wikidata.org/w/api.php"

        results = []
        for _ in range(3):
            try:
                response = requests.get(url, params=params)
                results = response.json()['query']['search']
                break
            except Exception as e:
                traceback.print_exc()
                time.sleep(3)

        # Extract QIDs and their corresponding ES scores
        qid_results = []
        score_results = []

        for idx in range(len(results)):
            qid = results[idx]['title']
            score = idx
            qid_results.append(qid)
            score_results.append(score)

        return qid_results, score_results

    def batch_retrieve_comparative(self,
                                   queries_batch,
                                   comparative_batch,
                                   K=50,
                                   Language=None):
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
        qids = []
        scores = []

        for query in queries_batch:
            result = self.get_similar_qids(query, K)
            qids.append(result[0])
            scores.append(result[1])

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
        qids = []
        scores = []

        for query in queries_batch:
            result = self.get_similar_qids(query, K)
            qids.append(result[0])
            scores.append(result[1])

        return qids, scores