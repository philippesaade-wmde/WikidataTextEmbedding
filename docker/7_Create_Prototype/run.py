import json
import os
import hashlib
import time

from datasets import load_dataset
from datetime import datetime
from multiprocessing import Process, Queue, Manager
from tqdm import tqdm
from types import SimpleNamespace

from src.wikidataEmbed import WikidataTextifier
from src.wikidataRetriever import AstraDBConnect

MODEL = os.getenv("MODEL", "jinaapi")
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))

QUEUE_SIZE = 2 * EMBED_BATCH_SIZE * NUM_PROCESSES  # enough to not run out
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", QUEUE_SIZE))

DB_API_KEY_FILENAME = os.getenv("DB_API_KEY", "datastax_wikidata.json")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

CHUNK_NUM = os.getenv("CHUNK_NUM")

assert CHUNK_NUM is not None, (
    "Please provide `CHUNK_NUM` env var at docker run"
)

LANGUAGE = "en"
TEXTIFIER_LANGUAGE = "en"
DUMPDATE = "09/18/2024"

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not TEXTIFIER_LANGUAGE:
    TEXTIFIER_LANGUAGE = LANGUAGE

FILEPATH = f"../data/Wikidata/chunks/chunk_{CHUNK_NUM}.json.gz"

# wikidata_chunk_sizes_fname = "../data/Wikidata/chunk_sizes.json"
# TODO: Add location as env var
# TODO: Sync data format from DATADUMP to chunk_sizes.json
# TODO: Retrieve info from Hugging Face instead of storing it
wikidata_chunksizes_path = os.path.join("wikidata_chunk_sizes_2024-09-18.json")

with open(wikidata_chunksizes_path) as json_in:
    chunk_sizes = json.load(json_in)

total_entities = chunk_sizes[f"chunk_{CHUNK_NUM}"]

with open(f"../API_tokens/{DB_API_KEY_FILENAME}") as json_in:
    datastax_token = json.load(json_in)

dataset = load_dataset(
    "philippesaade/wikidata",
    data_files=f"data/chunk_{CHUNK_NUM}-*.parquet",
    streaming=True,
    split="train"
)


def process_items(queue, progress_bar):
    """Worker function that processes items from the queue
        and adds them to AstraDB.
    """
    with open(f"../API_tokens/{DB_API_KEY_FILENAME}") as json_in:
        datastax_token = json.load(json_in)

    graph_store = AstraDBConnect(
        datastax_token,
        COLLECTION_NAME,
        model=MODEL,
        batch_size=EMBED_BATCH_SIZE,
        cache_embeddings="wikidata_prototype"
    )
    textifier = WikidataTextifier(
        language=LANGUAGE,
        langvar_filename=TEXTIFIER_LANGUAGE
    )

    while True:
        item = queue.get()
        if item is None:
            break  # Exit condition for worker processes

        item_id = item['id']
        item_label = textifier.get_label(item_id, json.loads(item['labels']))
        item_description = textifier.get_description(
            item_id,
            json.loads(item['descriptions'])
        )
        item_aliases = textifier.get_aliases(json.loads(item['aliases']))

        if item_label is not None:
            # TODO: Verify: If label does not exist, then skip item
            entity_obj = SimpleNamespace()
            entity_obj.id = item_id
            entity_obj.label = item_label
            entity_obj.description = item_description
            entity_obj.aliases = item_aliases
            entity_obj.claims = json.loads(item['claims'])

            chunks = textifier.chunk_text(
                entity_obj,
                graph_store.tokenizer,
                max_length=graph_store.max_token_size
            )

            for chunk_i, chunk in enumerate(chunks):
                md5_hash = hashlib.md5(chunk.encode('utf-8')).hexdigest()
                metadata = {
                    "MD5": md5_hash,
                    "Label": item_label,
                    "Description": item_description,
                    "Aliases": item_aliases,
                    "Date": datetime.now().isoformat(),
                    "QID": item_id,
                    "ChunkID": chunk_i + 1,
                    "Language": LANGUAGE,
                    "IsItem": ('Q' in item_id),
                    "IsProperty": ('P' in item_id),
                    "DumpDate": DUMPDATE
                }

                graph_store.add_document(
                    id=f"{item_id}_{LANGUAGE}_{chunk_i+1}",
                    text=chunk,
                    metadata=metadata
                )

        progress_bar.value += 1

    while True:
        # Leftover Maintenance: Ensure that the batch is emptied out
        if not graph_store.push_batch():  # Stop when batch is empty
            break


if __name__ == "__main__":
    queue = Queue(maxsize=QUEUE_SIZE)
    progress_bar = Manager().Value("i", 0)

    with tqdm(total=total_entities) as pbar:
        processes = []
        for _ in range(NUM_PROCESSES):
            p = Process(target=process_items, args=(queue, progress_bar))
            p.start()
            processes.append(p)

        for item in dataset:
            queue.put(item)
            pbar.update(progress_bar.value - pbar.n)
            # pbar.n = progress_bar.value
            # pbar.refresh()

        for _ in range(NUM_PROCESSES):
            queue.put(None)

        while any(p.is_alive() for p in processes):
            pbar.update(progress_bar.value - pbar.n)
            # pbar.n = progress_bar.value
            # pbar.refresh()
            time.sleep(1)

        for p in processes:
            p.join()
