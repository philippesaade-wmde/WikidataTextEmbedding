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

if PREFIX != "":
    OUTPUT_FILENAME += PREFIX

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]
    print(f"API_KEY_FILENAME not provided. Using {API_KEY_FILENAME}")


with open(f"../API_tokens/{API_KEY_FILENAME}") as json_in:
    datastax_token = json.load(json_in)

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

OUTPUT_FILE_PATH = os.path.join("../data/Evaluation Data/",
                                f"{OUTPUT_FILENAME}.pkl")

outputfile_exists = os.path.exists(OUTPUT_FILE_PATH)
if not RESTART and outputfile_exists:
    # Load pre-existing evaluation data
    print(f"Loading data from: {OUTPUT_FILENAME}")
    with open(OUTPUT_FILE_PATH, "rb") as pkl_file:
        eval_data = pickle.load(pkl_file)
else:
    # If you run this for the first time, copy the evaluation data from origine.
    pkl_fpath = f"../data/Evaluation Data/{EVALUATION_PATH}"
    with open(pkl_fpath, "rb") as pkl_file:
        eval_data = pickle.load(pkl_file)

# If language is specified, take only the columns
# with queries in the specified language.
if 'Language' in eval_data.columns:
    eval_data = eval_data[eval_data['Language'] == QUERY_LANGUAGE]

# Setup the evaluation columns
if 'Retrieval QIDs' not in eval_data:
    eval_data['Retrieval QIDs'] = None
if 'Retrieval Score' not in eval_data:
    eval_data['Retrieval Score'] = None

# Get rows that are not already evaluated
def is_empty(x):
    return (x is None) or (len(x) == 0)
missing_qids = eval_data['Retrieval QIDs'].apply(is_empty)
missing_scores = eval_data['Retrieval Score'].apply(is_empty)
row_to_process = missing_qids | missing_scores
row_to_process_len = (~row_to_process).sum()


def run_evaluation_process():
    """Iterate over the queries in the evaluation dataset and retrieve the QIDs and Similarity Scores from the Vector Database.
    """
    print(f"Running: {OUTPUT_FILENAME}")

    with tqdm(total=len(eval_data), disable=False) as progressbar:
        progressbar.update(row_to_process_len)

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

            # TODO: Create progress bar update function
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
                with open(OUTPUT_FILE_PATH, "wb") as pkl_file:
                    pickle.dump(eval_data, pkl_file)

        with open(OUTPUT_FILE_PATH, "wb") as pkl_file:
            pickle.dump(eval_data, pkl_file)


if __name__ == "__main__":
    run_evaluation_process()
