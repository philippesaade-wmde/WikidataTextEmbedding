import json
import pandas as pd
import os
import pickle

from tqdm import tqdm
from src.wikidataRetriever import AstraDBConnect, KeywordSearchConnect

# TODO: change script to functional form with fucnctions called after __name__
MODEL = os.getenv("MODEL", "jina")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
API_KEY_FILENAME = os.getenv("API_KEY", None)
EVALUATION_PATH = os.getenv("EVALUATION_PATH")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

K = int(os.getenv("K", 50))
COMPARATIVE = os.getenv("COMPARATIVE", "false").lower() == "true"
COMPARATIVE_COLS = os.getenv("COMPARATIVE_COLS")
QUERY_COL = os.getenv("QUERY_COL")
QUERY_LANGUAGE = os.getenv("QUERY_LANGUAGE", 'en')
DB_LANGUAGE = os.getenv("DB_LANGUAGE", None)
RESTART = os.getenv("RESTART", "false").lower() == "true"
PREFIX = os.getenv("PREFIX", "")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH = os.getenv("ELASTICSEARCH", "false").lower() == "true"

OUTPUT_FILENAME = (
    f"retrieval_results_{EVALUATION_PATH.split('/')[-2]}-{COLLECTION_NAME}-"
    f"DB({DB_LANGUAGE})-Query({QUERY_LANGUAGE})"
)

# TODO: remove unneccesary commented out code
# OUTPUT_FILENAME = (
#     f"retrieval_results_{EVALUATION_PATH.split('/')[-2]}-"
#     f"keyword-search-{LANGUAGE}"
# )


if PREFIX != "":
    OUTPUT_FILENAME += PREFIX

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]
    print(f"API_KEY_FILENAME not provided. Using {API_KEY_FILENAME}")

datastax_token = json.load(open(f"../API_tokens/{API_KEY_FILENAME}"))

if ELASTICSEARCH:
    graph_store = KeywordSearchConnect(
        ELASTICSEARCH_URL,
        index_name=COLLECTION_NAME
    )
    OUTPUT_FILENAME += "_bm25"
else:
    graph_store = AstraDBConnect(
        datastax_token,
        COLLECTION_NAME,
        model=MODEL,
        batch_size=BATCH_SIZE,
        cache_embeddings=True
    )

# Load the Evaluation Dataset
if not QUERY_COL:
    raise ValueError("The QUERY_COL environment variable is required")
if not EVALUATION_PATH:
    raise ValueError("The EVALUATION_PATH environment variable is required")

outputfile_exists = os.path.exists(
    f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl"
)
if not RESTART and outputfile_exists:
    print(f"Loading data from: {OUTPUT_FILENAME}")
    pkl_fpath = f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl"
    with open(pkl_fpath, "rb") as pkl_file:
        eval_data = pickle.load(pkl_file)
else:
    pkl_fpath = f"../data/Evaluation Data/{EVALUATION_PATH}"
    with open(pkl_fpath, "rb") as pkl_file:
        eval_data = pickle.load(pkl_file)

if 'Language' in eval_data.columns:
    eval_data = eval_data[eval_data['Language'] == QUERY_LANGUAGE]

if __name__ == "__main__":
    print(f"Running: {OUTPUT_FILENAME}")

    with tqdm(total=len(eval_data), disable=False) as progressbar:
        if 'Retrieval QIDs' not in eval_data:
            eval_data['Retrieval QIDs'] = None
        if 'Retrieval Score' not in eval_data:
            eval_data['Retrieval Score'] = None

        # TODO: Refactor this row_to_process to avoid nested .apply
        # Find rows that haven't been processed
        row_to_process = eval_data['Retrieval QIDs'].apply(
            lambda x: (x is None) or (len(x) == 0)
            ) | eval_data['Retrieval Score'].apply(
                lambda x: (x is None) or (len(x) == 0)
            )

        pkl_output_path = f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl"
        progressbar.update((~row_to_process).sum())
        for i in range(0, row_to_process.sum(), BATCH_SIZE):
            batch_idx = eval_data[row_to_process].iloc[i:i+BATCH_SIZE].index
            batch = eval_data.loc[batch_idx]

            if COMPARATIVE:
                batch_results = graph_store.batch_retrieve_comparative(
                    batch[QUERY_COL],
                    batch[COMPARATIVE_COLS.split(',')],
                    K=K,
                    Language=DB_LANGUAGE
                )
            else:
                batch_results = graph_store.batch_retrieve(
                    batch[QUERY_COL],
                    K=K,
                    Language=DB_LANGUAGE
                )

            eval_data.loc[batch_idx, 'Retrieval QIDs'] = pd.Series(
                batch_results[0]
            ).values

            eval_data.loc[batch_idx, 'Retrieval Score'] = pd.Series(
                batch_results[1]
            ).values

            # TODO: Create progress bar update funciton
            # tqdm is not wokring in docker compose. This is the alternative
            progressbar.update(len(batch))
            tqdm.write(
                progressbar.format_meter(
                    progressbar.n,
                    progressbar.total,
                    progressbar.format_dict["elapsed"]
                )
            )
            if progressbar.n % 100 == 0:
                # BUG: Why is the pkl output file being written twice at end
                with open(pkl_output_path, "wb") as pkl_file:
                    pickle.dump(eval_data, pkl_file)

        # BUG: Why is the pkl output file being written twice at end
        pickle.dump(eval_data, open(pkl_output_path, "wb"))
