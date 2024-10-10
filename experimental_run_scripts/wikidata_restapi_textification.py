"""python
!pip install -U sentence-transformers
!pip install requests  # google-search-results beautifulsoup4
# !pip install langchain_community langchain
"""

import os
import json
import logging
# import requests  # For making HTTP requests.
import pandas as pd  # Create output DF and save to csv
import urllib  # For opening and reading URLs.
import sys

# from bs4 import BeautifulSoup  # For parsing HTML and extracting information.

# For performing Google searches using SerpApi.
# from serpapi import GoogleSearch

from functools import partial  # For partial function application.

# For threading-based parallelism.
from multiprocessing import Pool, cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from sentence_transformers import SentenceTransformer

# For removing duplicate elements in an array and type checking
from numpy import unique, ndarray
from time import time
from tqdm import tqdm  # For displaying progress bars in loops.

try:
    from google.colab import userdata
    USE_LOCAL = False
except Exception as e:
    print('USE_LOCAL = True')
    USE_LOCAL = True


class WikidataTextification:
    # Logger
    @staticmethod
    def get_logger(name):
        # if logger.get(name):
        #     return loggers.get(name)
        # else:
        # Create a logger
        logging.basicConfig(
            filename='wdchat_api.log',
            encoding='utf-8',
            level=logging.DEBUG
        )

        logger = logging.getLogger(name)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger.setLevel(logging.DEBUG)  # Set the logging level
        logger.propagate = False

        # Create console handler and set level to debug
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def __init__(
            self, embedder=None, lang='en', timeout=10, n_cores=cpu_count(),
            version=0, verbose=False, wikidata_base='"wikidata.org"',
            return_list=True, save_filename=None):

        n_cores = max(n_cores, cpu_count() - 1) if USE_LOCAL else n_cores

        # Initialize the logger for this module.
        self.logger = self.get_logger(__name__)
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

    # Retreival Pipeline
    def get_json_from_wikidata(self, thing_id, thing='items', key=None):
        """
        Retrieves JSON data from the Wikidata API for a specified item or property.

        Args:
            thing_id (str): The ID of the item or property to retrieve.
            thing (str): The type of thing to retrieve ('items' or 'properties').
            key (str, optional): A specific part of the data to retrieve.

        Returns:
            tuple: A tuple containing the JSON data and the final URL used for the API request.
        """
        user_agent = 'CoolBot/0.0 (https://example.org/coolbot/; coolbot@example.org)'

        # Adjust the API URL if it ends with 'wiki' by removing
        #   the last 3 characters.
        api_url = self.WIKIDATA_API_URL
        api_url = api_url[:-3] if api_url[:-3] == 'wiki' else api_url

        # Construct the URL for the API request.
        entity_restapi = 'rest.php/wikibase/v0/entities'
        thing_url = '/'.join([api_url, entity_restapi, thing, thing_id])

        # Add additional parts to the URL if 'key' and 'lang' are specified.
        if key is not None:
            thing_url = '/'.join([thing_url, key])

            if self.lang is not None:
                thing_url = '/'.join([thing_url, self.lang])

        for counter in range(self.timeout):
            if 'items//' in thing_url:
                if self.verbose:
                    self.logger.debug("'items//' in thing_url")

                # Return empty result if the URL is malformed.
                return {}, thing_url

            try:
                # Open the URL and read the response.
                with urllib.request.urlopen(thing_url) as j_inn:
                    # j_inn.headers = j_inn.headers | self.headers
                    for key, val in self.headers.items():
                        j_inn.headers[key] = val

                    assert ('Authorization' in j_inn.headers), (
                        'Authorization not in j_inn.headers'
                    )

                    get_code = j_inn.getcode()

                    if get_code != self.GET_SUCCESS:
                        self.logger.debug([thing_id, thing, get_code])

                        # Return empty result if the URL is malformed.
                        return {}, thing_url

                    # Decode and parse the JSON data
                    self.thing_data = j_inn.read().decode('utf-8')
                    self.thing_data = json.loads(self.thing_data)

                # Parse the JSON data and return it along with the URL.
                itemnotfound = 'item-not-found'

                is_found = False
                is_dict = isinstance(self.thing_data, dict)
                code_in_jdata = 'code' in self.thing_data
                if is_dict and code_in_jdata:
                    is_found = itemnotfound in self.thing_data['code']

                if code_in_jdata and is_found:
                    self.logger.debug(
                        'code in json_data and '
                        'itemnotfound in json_data["code"]'
                    )

                    # Return empty result if the URL is malformed.
                    return {}, thing_url

                return self.thing_data, thing_url

            except urllib.error.HTTPError as e:
                if self.verbose:
                    self.logger.debug('urllib.error.HTTPError')
                    self.logger.debug(f"{e}: {thing_id} {thing}")

                return {}, thing_url

            except Exception as e:
                # Log errors if verbose mode is enabled.
                if self.verbose:
                    self.logger.debug(f"Error downloading {thing_url}: {e}")

            if counter + 1 == self.timeout:
                self.logger.debug(
                    f"Timout({counter}) reached; Error downloading "
                )
                self.logger.debug(f"{thing}:{thing_id}:{key}:{thing_url}")

                return {}, thing_url

            # counter = counter + 1  # Increment the counter for each attempt.

        # Log if the function exits the loop without returning.
        self.logger.debug("End up with None-thing")

        return {}, thing_url

    def get_item_from_wikidata(self, qid, key=None, verbose=False):
        """
        Fetches JSON data for a specified Wikidata item using its QID.

        Args:
            qid (str): The unique identifier for the Wikidata item.
            key (str, optional): A specific part of the item data to retrieve. Defaults to None.

        Returns:
            tuple: A tuple containing the item JSON data and the URL used for the API request.
        """

        # Fetch JSON data from Wikidata using the general-purpose
        #   function get_json_from_wikidata.
        item_json, item_url = self.get_json_from_wikidata(
            thing_id=qid,
            thing='items',
            key=key,
        )

        # if isinstance(self.thing_data, str):
        #     logger.debug(f'{self.thing_data=}')

        # If the JSON data is not empty, return it along with the URL.
        if not len(item_json):
            item_json = {}

        return item_json, item_url

    def get_property_from_wikidata(self, pid, key=None):
        """
        Fetches JSON data for a specified Wikidata property using its PID.

        Args:
            pid (str): The unique identifier for the Wikidata property.
            key (str, optional): A specific part of the property data to retrieve. Defaults to None.

        Returns:
            tuple: A tuple containing the property JSON data and the URL used for the API request.
        """
        # Fetch JSON data from Wikidata using the general-purpose
        #   function get_json_from_wikidata.
        property_json, property_url = self.get_json_from_wikidata(
            thing_id=pid,
            thing='properties',
            key=key,
        )

        # If the JSON data is not empty, return it along with the URL.
        if not len(property_json):
            property_json = {}

        return property_json, property_url

    def download_and_extract_items(self, qids):
        """
        Downloads and extracts item information from a list of Wikidata URLs.

        Args:
            qids (list): A list of qids to process.

        Returns:
            list: A list of dictionaries containing item information extracted from each URL.
        """

        if not hasattr(self, 'items'):
            self.items = []

        self.counter = 0
        # Construct the base URL for item QIDs.
        qid_base = f'{self.WIKIDATA_UI_URL}/Q'

        if not isinstance(qids, (list, tuple, ndarray)):
            qids = list(qids)

        for k, qid_ in enumerate(qids):
            self.logger.debug(f'{k}: {qid_}')
            try:
                # Fetch item JSON data from Wikidata using the QID.
                item_json, item_url = self.get_item_from_wikidata(qid=qid_)

                # Skip processing if no item data is found.
                if len(item_json) == 0:
                    if self.verbose:
                        self.logger.debug('len(item_json) == 0')
                    continue

                item = {
                    'html_url': qid_,  # Wikidata URL.
                    'item_url': item_url,  # API URL to fetch the item.
                    # item data extracted from response.
                    'item_data': item_json,
                }

                # Append a dictionary with item details to the items list.
                if self.version == 0:
                    self.items.append(item)

                # Convert each item fetched from Wikidata into statements.
                if self.version == 1:
                    statements_ = self.convert_wikidata_item_to_statements(
                        item_json=item
                    )
                    self.wikidata_statements.extend(statements_)

                    # Create or Add DataFrame
                    if not hasattr(self, 'df_vecdb') or len(self.df_vecdb) == 0:
                        self.logger.debug('Creating self.df_vecdb')
                        self.df_vecdb = pd.DataFrame(statements_)
                    else:
                        self.logger.debug('Appending self.df_vecdb')
                        self.df_vecdb = pd.concat([
                            self.df_vecdb, pd.DataFrame(statements_)
                        ]).reset_index(drop=True)

                    self.logger.debug(f'{self.df_vecdb.shape}')

                # self.wikidata_statements.extend(
                #     self.convert_wikidata_item_to_statements(item_json=item)
                # )

            except Exception as e:
                # Log any exceptions that occur during processing.
                self.logger.debug(f'Failed to process {qid_}: {e}')

    # Textification
    def check_and_return_value(self, value, key):
        value_content = ''

        while isinstance(value, list):
            if len(value) > 1:
                self.logger.debug(f'{len(value)=}')

            value = value[0]

        try:
            if isinstance(value_content, dict):
                value_content = value[key]
            if isinstance(value_content, str):
                value_content = value
        except Exception as e:
            self.logger.debug(f'{e}: with {value_content=}')

        while isinstance(value, list):
            if len(value) > 1:
                self.logger.debug(f'{len(value)=}')

            value_content = value[0]

        if isinstance(value_content, dict):
            value_content = value_content[key]

        return value_content

    def convert_value_to_string(self, wikidata_statement, property_label):
        """
        Converts a Wikidata statement's value to a string based on its data type.

        Args:
            wikidata_statement (dict): The Wikidata statement containing the value and data type.
            property_label (str): The label of the property for the statement.

        Returns:
            tuple: A tuple containing the updated property label, value content, and raw value.
        """

        # Extracting the data type of the property.
        wikidata_data_type = wikidata_statement['property']['data-type']

        value = ''  # Initializing value and value content.
        value_content = ''
        if 'value' in wikidata_statement:  # Checking if the statement has a value.
            if 'content' in wikidata_statement['value']:
                # Checking if the value has content.
                value = wikidata_statement['value']['content']

        # Processing the value based on its data type and
        #   updating the property label accordingly.
        if wikidata_data_type == 'wikibase-item':
            # Fetching the item JSON for the value if it's a Wikibase item.
            value_content, _ = self.get_item_from_wikidata(
                qid=value,
                key='labels',
            )

        elif wikidata_data_type == 'time':
            value_content = self.check_and_return_value(value, 'time')
            assert not isinstance(
                value_content, list), f'value_content is a list'
            property_label = (
                f'has more information to be found at the {property_label}'
            )

        elif wikidata_data_type == 'external-id':
            property_label = (
                f'can be externally identified by the {property_label} as'
            )

        elif wikidata_data_type == 'commonsMediaid':
            property_label = (
                f'has the commonsMediaid of {property_label}'
            )

        elif wikidata_data_type == 'url':
            try:
                property_label = property_label.replace(' ', '_')
            except Exception as e:
                self.logger.debug(
                    f'{e}: {type(property_label)}: {property_label}')

            property_label = (
                f'has more information to be found at {property_label}'
            )

        elif wikidata_data_type == 'quantity':
            value_content = self.check_and_return_value(value, 'amount')
            assert (not isinstance(value_content, list)
                    ), f'value_content is a list'
            property_label = (
                f'has the quantity of {property_label} at'
            )

        elif wikidata_data_type == 'monolingualtext':
            lang_ = self.check_and_return_value(value, 'language')
            value_content = self.check_and_return_value(value, 'text')
            assert (not isinstance(value_content, list)
                    ), f'value_content is a list'
            property_label = (
                f'has the {lang_} monolingual text identifier'
                f' of {property_label} at'
            )

        # elif wikidata_data_type == 'English':
        #     value_content = value['text']
        #     property_label = (
        #         f'has the {lang_} monolingual text identifier'
        #         f' of {property_label} at'
        #     )

        if value_content == {}:
            value_content = ''

        # Return the updated property label, value content, and the raw value.
        assert (not isinstance(value_content, list)), (
            f'value_content is a list',
            value_content
        )

        assert (not isinstance(value_content, dict)), (
            f'value_content is a dict',
            value_content
        )

        return property_label, value_content, value

    def make_statement(
            self, prop_input: str, item_label: str = None,
            qid: str = None, key: str = None):
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
        self.counter = self.counter + 1
        # Unpacking the property ID and properties.
        pid, properties = prop_input

        # Fetching the property label from Wikidata.
        property_label, _ = self.get_property_from_wikidata(pid, key='labels')

        if len(property_label) == 0:
            return []  # Skip this one

        statements = []  # Initializing a list to store constructed statements.
        for wikidata_statement_ in properties:
            # Converting each Wikidata statement to a textual statement.
            property_label, value_content, value = self.convert_value_to_string(
                wikidata_statement=wikidata_statement_,
                property_label=property_label,
            )

            # Skipping if no property label is found.
            if len(value_content) == 0:
                continue  # Skipping statements with no value content.

            statement_ = ''  # Initializing the statement text.

            try:
                # Constructing the statement text.
                statement_ = ' '.join(
                    [item_label, property_label, value_content]
                )

                if self.verbose:
                    self.logger.debug(statement_)

            except Exception as e:
                # Logging any exceptions.
                self.logger.debug(f'Found Error: {e}')

                if self.verbose:
                    self.logger.debug([
                        wikidata_statement_['property']['data-type'],
                        item_label,
                        property_label,
                        value_content
                    ])

            # Appending the constructed statement information to the list.

            embedding_ = None
            if self.embedder is not None:
                embedding_ = self.embedder.encode(statement_)

            statements.append({
                'qid': qid,
                'pid': pid,
                'value': value if isinstance(value, str) else value_content,
                'item_label': item_label,
                'property_label': property_label,
                'value_content': value_content,
                'statement': statement_,
                'embedding': embedding_
            })

        return statements  # Returning the list of constructed statements.

    def convert_wikidata_item_to_statements(self, item_json: dict = None):
        """
        Converts a Wikidata item JSON into structured statements.

        Args:
            item_json (dict, optional): JSON data of a Wikidata item.

        Returns:
            list or str: A list of structured statements if 'return_list' is True, otherwise a string.
        """

        # Defaulting 'item_json' to an empty dict if not provided.
        if item_json is None:
            item_json = {}

        # Extracting essential item data from 'item_json'.
        qid = item_json['item_data']['id']
        item_label = item_json['item_data']['labels'][self.lang]
        item_desc = item_json['item_data'].get(
            'descriptions', {}).get(self.lang, '')

        # Constructing an initial statement describing the item.
        desc_statement = f'{item_label} can be described as {item_desc}'
        desc_embedding = None
        if self.embedder is not None:
            desc_embedding = self.embedder.encode(desc_statement)

        statements = [{
            'qid': qid,
            'pid': 'description',
            'value': item_desc,
            'item_label': item_label,
            'property_label': 'can be described as',
            'value_content': item_desc,
            'statement': desc_statement,
            'embedding': desc_embedding
        }]

        # Processing each statement associated with the item in parallel.
        item_statements = item_json['item_data']['statements']

        item_pool = partial(
            self.make_statement,
            qid=qid,
            item_label=item_label,
        )

        with ThreadPool(self.n_cores) as pool:
            # Wrap pool.imap with tqdm for progress tracking
            pool_imap = pool.imap(item_pool, item_statements.items())
            results = list(tqdm(pool_imap, total=len(item_statements.items())))

        for res_ in results:
            if res_ is None:
                continue
            statements.extend(res_)

        return statements

    def get_wikidata_statements(self, qids):
        """
        Retrieves structured statements from Wikidata based on a given query.

        Args:
            qids (list): a list of qids to access from Wikidata.

        Returns:
            list or str: A list of statements if return_list is True; otherwise, a string of concatenated statements.
        """

        if not hasattr(self, 'wikidata_statements'):
            # Initialize an empty list to store the statements.
            self.wikidata_statements = []

        if not hasattr(self, 'df_vecdb'):
            self.df_vecdb = pd.DataFrame()

        # Fetch and override items from Wikidata from the list of QIDs
        self.download_and_extract_items(qids)

        """
        # Override existing wikidata_items to minimise RAM impact
        self.wikidata_items = [] if self.items is None else self.items

        if not hasattr(self, 'wikidata_statements'):
            # Initialize an empty list to store the statements.
            self.wikidata_statements = []
        """

        # Convert each item fetched from Wikidata into statements.
        if self.version == 0:
            for wikidata_item_ in self.items:  # self.wikidata_items:
                statements_ = self.convert_wikidata_item_to_statements(
                    item_json=wikidata_item_
                )
                self.wikidata_statements.extend(statements_)

                # Create or Add DataFrame
                if not hasattr(self, 'df_vecdb') or len(self.df_vecdb) == 0:
                    self.logger.debug('Creating self.df_vecdb')
                    self.df_vecdb = pd.DataFrame(statements_)
                else:
                    self.logger.debug('Appending self.df_vecdb')
                    self.df_vecdb = pd.concat([
                        self.df_vecdb, pd.DataFrame(statements_)
                    ]).reset_index(drop=True)
                self.logger.debug(f'{self.df_vecdb.shape}')

        # Return the statements either as a list or as a concatenated string,
        #   based on the return_list flag.
        if not self.return_list:
            self.wikidata_statements = '\n'.join(
                [wds_['statement'] for wds_ in self.wikidata_statements]
            ).replace('\n\n', '\n')

        if self.save_filename is not None:
            self.df_vecdb.to_csv(self.save_filename)
