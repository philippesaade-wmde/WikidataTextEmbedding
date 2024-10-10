import bz2
import dask.dataframe as dd
import json
import logging
import os
import pandas as pd
import requests
import urllib  # For opening and reading URLs.
import uuid
import sqlite3
import subprocess
import sys
import torch

from sentence_transformers import SentenceTransformer
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from SPARQLWrapper import SPARQLWrapper, JSON
from time import time
from tqdm import tqdm
from urllib.request import urlopen

try:
    GPU_AVAILABLE = torch.cuda.is_available()
    print(f'{GPU_AVAILABLE=}')
except:
    GPU_AVAILABLE = False

try:
    from google.colab import userdata, drive
    drive.mount('/content/drive')

    USE_LOCAL = False
except Exception as e:
    print('USE_LOCAL = True')
    USE_LOCAL = True


def is_docker():
    """Check if the script is running inside a Docker container."""
    # Check for .dockerenv file
    if os.path.exists('/.dockerenv'):
        return True

    # Check for Docker-specific entries in /proc/1/cgroup
    try:
        with open('/proc/1/cgroup', 'rt') as f:
            for line in f:
                if 'docker' in line:
                    return True
    except Exception:
        pass

    return False


def embedd_jina_api(statement):
    jina_api_key = os.getenv('JINA_API_KEY')

    url = 'https://api.jina.ai/v1/embeddings'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': (
            f'Bearer {jina_api_key}'
        )
    }

    data = {
        'input': [statement],
        'model': 'jina-embeddings-v2-base-en'
    }

    response = requests.post(url, headers=headers, json=data)
    return json.loads(
        response.content.decode('utf-8')
    )['data'][0]['embedding']


def push_datastax_api(statements):
    jina_api_key = os.getenv('JINA_API_KEY')

    url = 'https://api.jina.ai/v1/embeddings'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': (
            f'Bearer {jina_api_key}'
        )
    }

    data = {
        'input': [statement],
        'model': 'jina-embeddings-v2-base-en'
    }

    response = requests.post(url, headers=headers, json=data)
    return json.loads(
        response.content.decode('utf-8')
    )['data'][0]['embedding']


class WikidataRESTAPI:
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
            self, lang='en', timeout=1, verbose=False,
            wikidata_base='"wikidata.org"'):

        # Initialize the logger for this module.
        self.logger = self.get_logger(__name__)

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

        self.lang = lang
        # self.timeout = timeout
        self.verbose = verbose
        self.wikidata_base = wikidata_base

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

        # for counter in range(self.timeout):
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

            # if counter + 1 == self.timeout:
            #     self.logger.debug(
            #         f"Timout({counter}) reached; Error downloading "
            #     )
            #     self.logger.debug(f"{thing}:{thing_id}:{key}:{thing_url}")

            #     return {}, thing_url

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

        return item_json  # , item_url

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

        return property_json  # , property_url


def get_value_label(value, conn=None):
    # print(f'get_value_label({value=}, conn={conn=})')
    value_label = value
    if isinstance(value, dict):
        if 'id' in value:
            value_label = value['id']
        if 'amount' in value:
            value_label = value['amount']
        if 'time' in value:
            value_label = value['time']
        if 'text' in value:
            value_label = value['text']
        if 'latitude' in value:
            value_label = f'lat{value["latitude"]}'
            if 'longitude' in value:
                value_label = (
                    f'{value_label}_lon{value["longitude"]}'
                )
            if 'altitude' in value:
                value_label = (
                    f'{value_label}_alt{value["altitude"]}'
                )
        if 'entity-type' in value and value['entity-type'] == 'item':
            value = value['id']

        if isinstance(value_label, dict):
            if 'entity-type' not in value.keys():
                print(f'{value=}')
            return

    if conn is not None:
        # print(f'Call value if QID: {value=}')
        is_qid = False
        if is_string := isinstance(value, str):
            is_qid = value[0] == 'Q' and value[1:].isdigit()
            # print(f'{is_string=}, {is_qid=}, {value}')

        if is_qid:
            value_label = query_label(conn, value, field='qid')
            if value_label is not None:
                value_label = value_label[1]
                if value_label[:2] == "b'":
                    value_label = value_label[2:]
                if value_label[-1] == "'":
                    value_label = value_label[:-1]

    # If value is not QID, then return value as value_label
    if value_label is not None:
        return value_label.replace('"', "\'")


def get_property_label(pid_, conn=None):
    prop_label = pid_
    if conn is not None:
        prop_label = query_label(conn, pid_, field='pid')
        if prop_label is not None:
            prop_label = prop_label[1]
        else:
            print(f'{pid_=}, {prop_label=}')

    if prop_label is not None:
        return prop_label.replace('"', "\'")


def convert_props_to_string(conn, pid, claimlist):

    item_str = ''
    pid_curr = None
    for claim_ in claimlist:
        # value = None  # Default to None
        # statement_ = None  # Default to None
        if 'datavalue' in claim_['mainsnak'].keys():
            value = claim_['mainsnak']['datavalue']['value']
            is_dict = isinstance(value, dict)
            value_label = get_value_label(value, conn)

            if value_label is None:
                continue

            # if is_dict:
            #     value = value_label

            # if isinstance(value, str):
            #     value = value.replace('"', "\'")

            prop_label = get_property_label(pid, conn)

            # statement_ = f'{item_desc} {prop_label} {value_label}'
            # statement_ = statement_.replace('"', "\'")
            if pid_curr is not None and pid == pid_curr:
                item_str = item_str + f',{value_label}'
                pid_curr = pid
            else:
                item_str = item_str + f'\n{prop_label}: {value_label}'

    return item_str


def chunk_item_string(
        item_str, qid, header='', n_statements=None,
        n_sitelinks=None, n_descriptions=None, chunksize=100, len_header=2):

    # Chunking procedure
    chunks_dict = []
    chunks = []
    k_chunk = 0

    split_lines = item_str.split('\n')
    n_lines = len(split_lines)

    for line_ in split_lines:
        chunks.append(line_)

        if len(chunks) < chunksize:
            continue

        chunk_str = header + '\n'.join(chunks)
        chunks_dict.append({
            'qid': qid,
            'chunk_id': k_chunk,
            'qid_chunk': f'{qid}_{k_chunk}',
            'n_statements': n_statements,
            'n_sitelinks': n_sitelinks,
            'n_descriptions': n_descriptions,
            'n_lines': n_lines,
            'item_str': chunk_str,
            'uuid': str(uuid.uuid4()),  # for db uniqueness
            'embedding': None
        })

        k_chunk = k_chunk + 1
        chunks = []

    if len(chunks):
        k_chunk = k_chunk + 1
        chunk_str = header + '\n'.join(chunks)
        chunks_dict.append({
            'qid': qid,
            'chunk_id': k_chunk,
            'qid_chunk': f'{qid}_{k_chunk}',
            'n_statements': n_statements,
            'n_sitelinks': n_sitelinks,
            'n_descriptions': n_descriptions,
            'n_lines': n_lines,
            'item_str': chunk_str,
            'uuid': str(uuid.uuid4()),  # for db uniqueness
            'embedding': None
        })
        chunks = []

    return chunks_dict


def entity_to_item_chunks(
        entity, conn=None, chunksize=100, lang='en', len_header=2):

    n_sitelinks = len(entity['sitelinks'])
    n_descriptions = len(entity['descriptions'])

    qid_ = entity['id']

    qid_label = qid_
    if conn is not None:
        qid_label = query_label(conn, qid_, field='qid')
        if qid_label is not None:
            qid_label = qid_label[1]
            if qid_label[:2] == "b'":
                qid_label = qid_label[2:]
            if qid_label[-1] == "'":
                qid_label = qid_label[:-1]
        qid_label = qid_label.replace('"', "\'")

    item_desc = entity['descriptions'][lang]['value']
    item_desc = item_desc.replace('"', "\'")

    # item_dicts = []
    header = f'Label: {qid_label}\n'
    header = header + f'Description: {item_desc}\n'

    item_str = ''

    # TODO: Create aliases list
    # item_str = item_str + f'Aliases: ' + ', '.join(aliases) + '\n'
    claims = entity['claims'].items()
    n_statements = sum(len(claim) for claim in claims)
    # print(f'{len(claims)=}, {n_statements=}')

    for prop_claims_ in claims:  # tqdm(
        item_str = item_str + convert_props_to_string(conn, *prop_claims_)

    return chunk_item_string(
        item_str=item_str,
        qid=qid_,
        header=header,
        n_statements=n_statements,
        n_sitelinks=n_sitelinks,
        n_descriptions=n_descriptions,
        chunksize=chunksize,
        len_header=len_header
    )


def embed_items(item_dicts):

    # item_from_dict = []

    item_batch = []
    embeddings_for_dict = []
    for item_ in tqdm(item_dicts):
        item_batch.append(item_)
        if len(item_batch) >= batchsize:
            # embedding_ = embedd_jina_api(item_from_dict)
            embedding_ = embedder.encode(item_batch)
            embeddings_for_dict.extend(embedding_)

            item_batch = []

    if len(item_batch):
        # embedding_ = embedd_jina_api(item_from_dict)
        embedding_ = embedder.encode(item_batch)
        embeddings_for_dict.extend(embedding_)
        item_batch = []

    dict_list_out = []
    for line_, embed_ in zip(item_dicts, embeddings_for_dict):
        line_['embedding'] = embed_
        dict_list_out.append(line_)

    # Reset variable pointer and remove extra MEM usage
    return dict_list_out


def write_dict_list_to_file(item_dicts, fout):
    for dict_ in item_dicts:
        line_ = ','.join([f'"{item_}"' for item_ in dict_.values()])
        line_ = f'{line_}\n'
        fout.write(line_)


def stream_etl_wikidata_datadump(
        in_filepath, fout, conn=None, embedder=None, batchsize=None,
        chunksize=100, lang='en', n_complete=None, qids_only=False):

    if batchsize is not None:
        item_dicts = []  # batching statements before embedding
    n_items = n_items if 'n_items' in locals() else 0
    n_statements = n_statements if 'n_statements' in locals() else 0

    print(f'Starting urlopen from {in_filepath}')
    with (urlopen(in_filepath) as stream, bz2.BZ2File(stream) as file):
        pbar = tqdm(enumerate(file))
        for k_iter, line in pbar:
            pbar_desc = (
                f'Counters: '
                f'n_items {n_items}'
                f' - n_statements: {n_statements}'
            )

            pbar.set_description(pbar_desc)
            pbar.refresh()  # to show immediately the update

            # if k_iter < n_items:
            #     continue

            if n_complete is not None and n_items > n_complete:
                if None not in [embedder, batchsize] and len(item_dicts):
                    # If batch embedding, then embed stack of dicts here
                    item_dicts = embed_items(item_dicts)
                    write_dict_list_to_file(item_dicts, fout)

                # Stop after `n_complete` items to avoid overloaded filesize
                break

            n_items = n_items + 1
            line = line.decode().strip()

            if line in {'[', ']'}:
                continue

            if line.endswith(','):
                line = line[:-1]

            entity = json.loads(line)

            if 'sitelinks' not in entity.keys():
                continue

            if lang not in entity['descriptions'].keys():
                continue

            # n_has_sitelinks = n_has_sitelinks + 1
            if qids_only:
                qid_ = entity['id']
                item_desc = entity['descriptions'][lang]['value']

                fout.write(f'{qid_},"{item_desc}"\n')
                n_statements = n_statements + 1
                continue

            # dict_vecdb.append(vecdb_line_)
            item_dict = entity_to_item_chunks(
                entity,
                conn=conn,
                chunksize=chunksize,
                lang=lang
            )

            if batchsize is not None:
                item_dicts.extend(item_dict)
            else:
                item_dicts = item_dict

            n_dicts = len(item_dicts)
            if None not in [embedder, batchsize] and n_dicts >= batchsize:
                # If batch embedding, then embed stack of dicts here
                item_dicts = embed_items(item_dicts)

            if batchsize is not None and n_dicts < batchsize:
                # If item_dicts len is less than batchsize
                #   continue to next iteration without saving yet
                continue

            write_dict_list_to_file(item_dicts, fout)
            n_statements = n_statements + n_dicts
            item_dicts = []


def grep_string_in_file(search_string, file_path):
    try:
        # Using subprocess.check_output to run grep command
        output = subprocess.check_output(
            ['grep', '-q', search_string, file_path],
            stderr=subprocess.STDOUT
        )
        return True
    except subprocess.CalledProcessError as e:
        # grep returns a non-zero exit status if the string is not found
        if e.returncode == 1:
            return False
        else:
            # Re-raise the exception if it's not the expected
            #   'not found' exit status
            raise e


def correct_qid_label_csv(in_filename, out_filename, n_test=10, max_iter=None):
    with open(in_filename, 'r') as f_inn:
        with open(out_filename, 'w') as f_out:
            header = f_inn.readline()
            header = header.replace("\n", "").split(',')
            header = ','.join([f'"{head_}"' for head_ in header])
            header = f'{header}\n'
            f_out.write(header)
            iterator = tqdm(enumerate(f_inn.readlines()), total=max_iter)
            for k, line in iterator:
                qid_labels = line.split(',')
                qid_ = qid_labels[0]

                label_ = ' '.join(qid_labels[1:]).replace('\n', '')
                label_ = label_.encode()

                f_out.write(f'{qid_},{label_}\n')
                if n_test is not None and k > n_test:
                    break


def query_qid_label(conn, qid):
    cur = conn.cursor()
    cur.execute(f"select * from qid_labels where qid == '{qid}';")
    return cur.fetchone()


def query_pid_label(conn, pid):
    cur = conn.cursor()
    cur.execute(f"select * from pid_labels where pid == '{pid}';")
    return cur.fetchone()


def query_label(conn, qpid, field='qid'):
    cur = conn.cursor()
    query = f"""select * from {field}_labels where {field} == '{qpid}';"""
    try:
        cur.execute(query)
    except Exception as e:
        print(f'Error: {e}')
        print(f'Query: {query}')

    return cur.fetchone()


def load_qid_label_csv(filename):
    # df_ = pd.read_csv(filename, encoding='latin1')
    df_ = dd.read_csv(filename).compute()
    df_['n_qid'] = df_['qid'].apply(lambda x: int(x.replace('Q', '')))
    # df_['label'] = df_['label'].apply(lambda x: x.encode().decode())
    # df_['label'] = df_['label'].str.decode('utf-8')
    df_['label'] = df_['label'].apply(lambda x: x[1:])
    df_.sort_values('n_qid', ascending=True, ignore_index=True, inplace=True)
    return df_.set_index('n_qid')


def confirm_overwrite(filepath):
    print(f'File exists: {filepath}')
    confirm = input("Confirm overwrite (y/c/[n])?: ")

    while True:

        if confirm.upper() == "Y":
            print(f"\nOverwriting file {filepath}\n")
            return True

        elif confirm.upper() == "N" or confirm == '':
            print(
                "File not selected for overwrite. Please change output filename"
            )
            return False
        elif confirm.upper() == "C":
            print("File selected for continue.")
            return None
        else:
            print(f'Input unrecognised. Please enter `y` or `n`')
            confirm = input("Confirm overwrite (y/[n])?: ")


def get_one_pid_label(n_pid_, verbose=False):

    if USE_LOCAL:
        WIKIMEDIA_TOKEN = os.environ.get('WIKIMEDIA_TOKEN')
    else:
        WIKIMEDIA_TOKEN = userdata.get('WIKIMEDIA_TOKEN')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {WIKIMEDIA_TOKEN}'
    }

    rest_api_uri = 'https://www.wikidata.org/w/rest.php/wikibase/v0/entities'
    prop_label_uri = f'{rest_api_uri}/properties/P{n_pid_}/labels/en'
    # label_ = requests.get(prop_label_uri).content.decode()
    try:
        with urllib.request.urlopen(prop_label_uri) as j_inn:
            for key, val in headers.items():
                j_inn.headers[key] = val

            assert ('Authorization' in j_inn.headers), (
                'Authorization not in j_inn.headers'
            )

            get_code = j_inn.getcode()
            res_ = j_inn.read().decode('utf-8')
            label_ = json.loads(res_)

        # if label_[0] == '{' and label_[-1] == '}':
        #     return {'pid': f'P{pid_}', 'label': None, 'n_pid': pid_}

        return {'pid': f'P{n_pid_}', 'label': label_, 'n_pid': n_pid_}
    except urllib.error.HTTPError as e:
        if verbose:
            print(f'Error: P{pid_}: {e}')

    # print("url is error")
    return {'pid': f'P{n_pid_}', 'label': None, 'n_pid': n_pid_}


def post_process_embed_df(df, embedder, batchsize=120):
    start = time()
    n_rows = df.index.size
    pbar = tqdm(df.iterrows(), total=n_rows)

    stack_rows = []
    for _, row_ in pbar:
        stack_rows.append(row_)
        if len(stack_rows) == batchsize:
            statements = [row_.statement for row_ in stack_rows]
            inds = [row_.name for row_ in stack_rows]
            embeddings_ = embedder.encode(statements)

            for ind_, embed_ in zip(inds, embeddings_):
                df.at[ind_, 'embedding'] = embed_.tolist()

            # Reset batch
            stack_rows = []

            ratio_done = (n_rows - df['embedding'].isnull().sum()) / n_rows
            pbar.set_description(f'{ratio_done:0.1%}')
            pbar.refresh()  # to show immediately the update

    if stack_rows:
        print(f'Wrapping up trailing {len(stack_rows)} rows.')
        statements = [row_.statement for row_ in stack_rows]
        inds = [row_.name for row_ in stack_rows]
        embeddings_ = embedder.encode(statements)

        for ind_, embed_ in zip(inds, embeddings_):
            df.at[ind_, 'embedding'] = embed_.tolist()

    print(f'Operation took {time() - start:.1f} seconds.')
    return df


def fix_df_pid_labels(df, max_pid=12727, n_cores=cpu_count()-1):
    npids_full = range(1, max_pid+1)
    n_pids = [npid_ for npid_ in npids_full if npid_ not in df.n_pid.values]

    len_n_pids = len(n_pids)
    print(f'{len_n_pids=}')

    with ThreadPool(n_cores) as pool:
        # Wrap pool.imap with tqdm for progress tracking
        pool_imap = pool.imap(get_one_pid_label, n_pids)
        pid_labels = list(tqdm(pool_imap, total=len_n_pids))
    # missing_pid_labels = fix_df_pid_labels(df_pid_labels)
    return pid_labels


def get_all_pid_labels(max_pid=12727, n_cores=cpu_count()-1, filename=None):
    pids = range(1, max_pid+1)
    with ThreadPool(n_cores) as pool:
        # Wrap pool.imap with tqdm for progress tracking
        pool_imap = pool.imap(get_one_pid_label, pids)
        pid_labels = list(tqdm(pool_imap, total=max_pid))

    df = pd.DataFrame(pid_labels).dropna()

    if filename is not None:
        df_pid_labels.to_csv(filename)

    return df.sort_values('n_pid', ignore_index=True)


def sparql_list_all_pid_labels(
        wikidata_sparql_endpoint="https://query.wikidata.org/sparql"):
    # Set up the SPARQL endpoint and the query
    sparql = SPARQLWrapper(wikidata_sparql_endpoint)
    sparql.setQuery("""
    SELECT ?property ?propertyLabel WHERE {
        ?property a wikibase:Property .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    """)

    # Set the return format to JSON
    sparql.setReturnFormat(JSON)

    # Execute the query and get the results
    results = sparql.query().convert()

    # Process the results
    properties = results["results"]["bindings"]

    # Extract and print the property labels
    pid_labels = []
    for prop in properties:
        pid = prop["property"]["value"].split('/')[-1]  # Extract the PID
        label = prop["propertyLabel"]["value"]  # Extract the label
        pid_labels.append({'pid': pid, 'label': label})

    return pd.DataFrame(pid_labels)


def process_wikidata_dump(
        out_filepath, in_filepath, embedder=None, batchsize=None,
        chunksize=100, lang='en', n_complete=None, qids_only=False,
        db_name='sqlitedbs/wikidata_qid_pid_labels.db'):

    warm_start = False
    if os.path.exists(out_filepath):
        overwrite = confirm_overwrite(out_filepath)
        warm_start = overwrite is None
        if not (warm_start or overwrite):
            sys.exit()

    if not warm_start:
        print('Creating Header')

        full_header = (
            'qid,chunk_id,qid_chunk,'
            'n_statements,n_sitelinks,n_descriptions,n_lines,'
            'item_str,uuid,embedding\n'
        )

        with open(out_filepath, 'w', newline='') as fout:  #
            header = 'qid,label\n' if qids_only else full_header
            fout.write(header)

    start = time()
    with open(out_filepath, 'a') as fout, sqlite3.connect(db_name) as conn:
        stream_etl_wikidata_datadump(
            in_filepath=in_filepath,
            fout=fout,
            conn=conn,
            embedder=embedder,
            batchsize=batchsize,
            chunksize=chunksize,
            lang=lang,
            n_complete=n_complete,
            qids_only=qids_only
        )

    print(f'Embedding store took {time() - start} seconds.')


def fetch_wikidata_labels(limit=10000, offset=0):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setReturnFormat(JSON)

    labels = []

    while True:
        query = f"""
            SELECT ?item ?itemLabel WHERE {{
                ?item wdt:P31 wd:Q5 .
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            }}
            LIMIT {limit} OFFSET {offset}
        """

        sparql.setQuery(query)
        results = sparql.query().convert()

        bindings = results["results"]["bindings"]
        if not bindings:
            break

        for result in bindings:
            qid = result["item"]["value"].split('/')[-1]  # Extract the QID
            label = result["itemLabel"]["value"]  # Extract the label
            labels.append({'qid': qid, 'label': label})

        offset += limit
        print(f"Fetched {len(labels)} items so far...")

    return labels


if __name__ == '__main__':
    from argparse import ArgumentParser
    import argparse

    parser = argparse.ArgumentParser(
        description='Arguments for Wikidata Data Dump Processes'
    )
    parser.add_argument(
        '--n_complete', '-nc', type=int, default=10,
        help='Number of Wikidata items to process from data dump.'
    )
    parser.add_argument(
        '--embed', '-e', action='store_true', default=False,
        help='Toggle to activate the embedder process.'
    )
    parser.add_argument(
        '--batchsize', '-eb', type=int, default=128,
        help='Number of Wikidata item chunks to embed in a single batch.'
    )
    parser.add_argument(
        '--chunksize', '-cs', type=int, default=100,
        help='Number of Wikidata statements per chunk.'
    )

    args = parser.parse_args()

    IS_DOCKER = is_docker()
    DL_STREAM = os.environ.get('STREAM', False)
    N_COMPLETE = int(os.environ.get('N_COMPLETE', args.n_complete))
    EMBED = os.environ.get('EMBED', args.embed)
    batchsize = os.environ.get('EMBED_BATCHSIZE', args.batchsize)
    chunksize = os.environ.get('CHUNKSIZE', args.chunksize)

    if chunksize is not None:
        chunksize = int(chunksize)

    if batchsize is not None:
        batchsize = int(batchsize)

    if isinstance(EMBED, str):
        EMBED = EMBED == 'True'

    embedder = None
    if EMBED:  # and 'embedder' not in locals()
        embedder = SentenceTransformer(
            "jinaai/jina-embeddings-v2-base-en",
            trust_remote_code=True
        )

    wikidata_datadump_filename = 'latest-all.json.bz2'

    if DL_STREAM:
        wikidata_datadump_path = (
            f'https://dumps.wikimedia.org/wikidatawiki/entities/'
            f'{wikidata_datadump_filename}'
        )
    elif IS_DOCKER:
        wikidata_datadump_path = (
            f'file:///app/datadump/{wikidata_datadump_filename}'
        )
    else:
        wikidata_datadump_path = (
            f'file:///home/jofr/Research/Wikidata/{wikidata_datadump_filename}'
        )

    out_filedir = './csvfiles/' if USE_LOCAL else '/content/drive/'
    out_filename = 'wikidata_vectordb_datadump_item_chunks_XYZ_en.csv'

    if IS_DOCKER:
        out_filedir = '/app/csvfiles/'

    out_filepath = os.path.join(out_filedir, out_filename)

    db_name = 'sqlitedbs/wikidata_qid_pid_labels.db'

    if IS_DOCKER:
        print("Running inside a Docker container.")
        db_name = f'/app/{db_name}'

    print(f'{db_name=}')
    print(f'{os.path.exists(db_name)=}')

    lang = 'en'
    qids_only = False
    batchsize = batchsize  # Set the batch size for embedding

    n_complete = N_COMPLETE

    if qids_only:
        out_filepath = out_filepath.replace('_XYZ_', '_qids_XYZ_')
        # Example 'csvfiles/wikidata_vectordb_datadump_qids_XYZ_en.csv'

    if n_complete is not None:
        out_filepath = out_filepath.replace('_XYZ_', f'_{n_complete}_')
        # Example f'csvfiles/wikidata_vectordb_datadump_qids_
        #   {n_complete}_en.csv'

    print(f'{out_filepath=}')
    print(f'{batchsize=}')
    print(f'{chunksize=}')

    """
    pid_labels_filename = 'wikidata_vectordb_datadump_qids_12723_en.csv'
    pid_labels_filename = f'csvfiles/{pid_labels_filename}'
    df_pid_labels = get_all_pid_labels(
        max_pid=12727,
        n_cores=cpu_count()-1,
        filename=pid_labels_filename
    )
    """
    """
    # Example usage
    labels = fetch_wikidata_labels(limit=10000)
    for qid, label in labels:
        print(f"{qid}: {label}")
    """
    process_wikidata_dump(
        out_filepath=out_filepath,
        in_filepath=wikidata_datadump_path,
        db_name=db_name,
        embedder=embedder,  # Test post-process embedding
        batchsize=batchsize,
        chunksize=chunksize,
        lang=lang,
        n_complete=n_complete,
        qids_only=qids_only
    )
