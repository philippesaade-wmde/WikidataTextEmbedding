"""
pip install SPARQLWrapper
pip install -U sentence-transformers
# !pip install google-search-results requests # beautifulsoup4
"""

import logging
import os
import sys
import pandas as pd

from functools import partial
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from sentence_transformers import SentenceTransformer
from SPARQLWrapper import SPARQLWrapper, JSON
from time import time
from tqdm import tqdm

try:
    from google.colab import userdata
    USE_LOCAL = False
except Exception as e:
    print('USE_LOCAL = True')
    USE_LOCAL = True


class WikidataTextification:
    # Logger
    # @staticmethod
    # def get_logger(name):
    #     # if logger.get(name):
    #     #     return loggers.get(name)
    #     # else:
    #     # Create a logger
    #     logging.basicConfig(
    #         filename='wdchat_api.log',
    #         encoding='utf-8',
    #         level=logging.DEBUG
    #     )

    #     logger = logging.getLogger(name)

    #     if logger.hasHandlers():
    #         logger.handlers.clear()

    #     logger.setLevel(logging.DEBUG)  # Set the logging level
    #     logger.propagate = False

    #     # Create console handler and set level to debug
    #     handler = logging.StreamHandler(sys.stdout)
    #     handler.setLevel(logging.DEBUG)
    #     formatter = logging.Formatter(
    #         '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    #     )
    #     handler.setFormatter(formatter)
    #     logger.addHandler(handler)

    #     return logger

    def __init__(
            self, embedder=None, lang='en', timeout=10, n_cores=cpu_count(),
            version=0, verbose=False, wikidata_base='"wikidata.org"',
            return_list=True, save_filename=None):
        print('WikidataTextification Initiated')
        n_cores = max(n_cores, cpu_count() - 1) if USE_LOCAL else n_cores

        # Initialize the logger for this module.
        # self.logger = self.get_logger(__name__)
        self.version = version

        # Base URL for Wikidata API, with a default value.
        self.WIKIDATA_API_URL = os.environ.get(
            'WIKIDATA_API_URL',
            'https://www.wikidata.org/w'
        )

        self.WIKIDATA_UI_URL = os.environ.get(
            'WIKIDATA_UI_URL',
            'https://www.wikidata.org/wiki'
        )

        self.WIKIDATA_SPARQL_ENDPOINT = os.environ.get(
            'WIKIDATA_SPARQL_ENDPOINT',
            'https://query.wikidata.org/sparql'
        )

        if USE_LOCAL:
            self.WIKIMEDIA_TOKEN = os.environ.get('WIKIMEDIA_TOKEN')
        else:
            self.WIKIMEDIA_TOKEN = userdata.get('WIKIMEDIA_TOKEN')

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.WIKIMEDIA_TOKEN}'
        }

        self.GET_SUCCESS = 200

        self.save_filename = save_filename
        self.embedder = embedder
        self.lang = lang
        self.timeout = timeout
        self.n_cores = n_cores
        self.verbose = verbose
        self.wikidata_base = wikidata_base
        self.return_list = return_list

        self.qids_processed = []

    def get_sparql_query(self, qid):
        return """
        #All statements of an item containing another item (direct / first-degree connections)
        #TEMPLATE={ "template": { "en": "All statements of ?item containing another item" }, "variables": { "?item": {} } }
        SELECT ?itemLabel ?propertyLabel ?property ?valueLabel ?value  WHERE {
        BIND(wd:""" + qid + """ AS ?item)
        ?item ?wdt ?value.
        ?property a wikibase:Property;
                wikibase:propertyType wikibase:WikibaseItem; # note: to show all statements, removing this is not enough, the graph view only shows entities
                wikibase:directClaim ?wdt.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        """

    def get_results(self, query):
        user_agent = (
            f"WDQS-example Python/"
            f"{sys.version_info[0]}.{sys.version_info[1]}"
        )

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.WIKIMEDIA_TOKEN}'
        }

        # TODO adjust user agent; see https://w.wiki/CX6
        sparql = SPARQLWrapper(
            self.WIKIDATA_SPARQL_ENDPOINT,
            agent=user_agent
        )
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        for header_name, header_val in self.headers.items():
            sparql.addCustomHttpHeader(
                httpHeaderName=header_name,
                httpHeaderValue=header_val
            )

        return sparql.query().convert()

    def make_statement(self, item_statement, qid):
        """
        Constructs a textual statement from a Wikidata property and its associated values.

        Args:
            prop_input (tuple): A tuple containing the property ID and the associated properties.
            item_label (str): The label of the Wikidata item.
            qid (str): The unique identifier of the Wikidata item. Optional.
            key (str): A specific part of the data to retrieve. Optional.

        Returns:
            list: A list of dictionaries containing statement information.
        """
        pid_ = item_statement.property
        value_ = item_statement.value
        item_label_ = item_statement.item_label
        property_label_ = item_statement.property_label
        value_content_ = item_statement.value_label
        statement_ = ' '.join([item_label_, property_label_, value_content_])

        statements = []  # Initializing a list to store constructed statements.

        embedding_ = None
        if self.embedder is not None:
            embedding_ = self.embedder.encode(statement_)

        return {
            'qid': qid,
            'pid': pid_,
            'value': value_,
            'item_label': item_label_,
            'property_label': property_label_,
            'value_content': value_content_,
            'statement': statement_,
            'embedding': embedding_
        }

    def sparql_to_dataframe(self, sparql_result):
        vec_meta = []
        for res_ in sparql_result['results']['bindings']:
            meta_ = {}
            for okey, oval in res_.items():
                for ikey, ival in oval.items():
                    nkey = f'{okey}_{ikey}' if ikey != 'value' else okey

                    oval_is_val_or_prop = okey in ['value', 'property']
                    ival_is_value = ikey == 'value'
                    http_in_ival = 'http' in ival

                    if oval_is_val_or_prop and ival_is_value and http_in_ival:
                        # Strip URI and return QID or PID
                        ival = ival.split('/')[-1]

                    nkey = nkey.replace(':', '_')
                    nkey = nkey.replace('Label', '_label')
                    meta_[nkey] = ival

            vec_meta.append(meta_)

        return pd.DataFrame(vec_meta)

    def item_to_vecdb(self, qid, df_item):
        # item_pool = partial(
        #     self.make_statement,
        #     qid=qid,
        # )

        df_item_rows = [row_[1] for row_ in df_item.iterrows()]
        # self.logger.debug(df_item_rows)

        results = []
        for row_ in df_item_rows:
            results.append(self.make_statement(row_, qid=qid))

        # with ThreadPool(self.n_cores) as pool:
        #     # Wrap pool.imap with tqdm for progress tracking
        #     pool_imap = pool.starmap(item_pool, zip(df_item_rows))
        #     # results = list(tqdm(pool_imap, total=len(df_item_rows)))
        #     results = list(pool_imap)

        statements = []
        for res_ in results:
            if res_ is None:
                continue

            if isinstance(res_, list):
                statements.extend(res_)

            if isinstance(res_, dict):
                statements.append(res_)

        # for df_row in df_item.iterrows():
        #     statements.append(make_statement(df_row[1], qid_, embedder))

        return pd.DataFrame(statements)

    def process_qid(self, qid_):
        print(f'WikidataTextification: {qid_}')
        has_df_vecdb = hasattr(self, 'df_vecdb')

        if has_df_vecdb:
            if 'qid' in self.df_vecdb.columns:
                if qid_ in self.df_vecdb.qid.unique():
                    return

        # self.logger.debug(f'{qid_=}')
        sparql_result = self.get_results(self.get_sparql_query(qid_))
        df_item = self.sparql_to_dataframe(sparql_result)

        df_ = self.item_to_vecdb(qid_, df_item)

        if not hasattr(self, 'df_vecdb'):  # or len(self.df_vecdb)
            self.df_vecdb = df_
        else:
            self.df_vecdb = pd.concat([
                self.df_vecdb, df_
            ]).reset_index(drop=True)

        self.qids_processed.append(qid_)

    def create_vecdb(self, qids):
        # for qid_ in tqdm(qids):
        with ThreadPool(self.n_cores) as pool:
            pool_imap = pool.starmap(self.process_qid, zip(qids))
            results = list(tqdm(pool_imap, total=len(qids)))
            #     # Wrap pool.imap with tqdm for progress tracking
            #     pool_imap = pool.starmap(item_pool, zip(df_item_rows))
            #     # results = list(tqdm(pool_imap, total=len(df_item_rows)))
            #     results = list(pool_imap)

        if self.save_filename is not None:
            self.df_vecdb.to_csv(self.save_filename)
