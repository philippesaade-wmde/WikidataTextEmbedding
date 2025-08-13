import json
import os
import time

from datasets import load_dataset
from datetime import datetime
from multiprocessing import Process, Queue, Manager
from tqdm import tqdm
from types import SimpleNamespace

from src.wikidataEmbed import WikidataTextifier
from src.wikidataRetriever import AstraDBConnect
from src.wikidataIDLogDB import WikidataIDLog

MODEL = os.getenv("MODEL", "jinaapi")
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))

QUEUE_SIZE = 2 * EMBED_BATCH_SIZE * NUM_PROCESSES  # enough to not run out
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", QUEUE_SIZE))

LANGUAGE = os.getenv("LANGUAGE", 'en')
TEXTIFIER_LANGUAGE = os.getenv("TEXTIFIER_LANGUAGE", None)
DUMPDATE = os.getenv("DUMPDATE", '09/18/2024')

DB_API_KEY_FILENAME = os.getenv("DB_API_KEY",
                                "datastax_wikidata.json")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

CHUNK_SIZES_PATH = os.getenv("CHUNK_SIZES_PATH",
                             "Wikidata/wikidata_chunk_sizes_2024-09-18.json")
CHUNK_NUM = os.getenv("CHUNK_NUM")

CHECK_IDS_PUSHED = os.getenv("CHECK_IDS_PUSHED", "false").lower() == "true"

assert CHUNK_NUM is not None, (
    "Please provide `CHUNK_NUM` env var at docker run"
)

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not TEXTIFIER_LANGUAGE:
    TEXTIFIER_LANGUAGE = LANGUAGE

if os.path.exists(f"../data/{CHUNK_SIZES_PATH}"):
    with open(f"../data/{CHUNK_SIZES_PATH}") as json_in:
        chunk_sizes = json.load(json_in)
    total_entities = chunk_sizes[f"chunk_{CHUNK_NUM}"]
else:
    total_entities = None


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
    )
    textifier = WikidataTextifier(
        language=LANGUAGE,
        langvar_filename=TEXTIFIER_LANGUAGE
    )

    while True:
        item = queue.get()
        progress_bar.value += 1
        if item is None:
            # Exit condition for worker processes
            break

        item_id = item['id']

        item_label = textifier.get_label(
            item_id,
            json.loads(item['labels'])
        )
        item_description = textifier.get_description(
            item_id,
            json.loads(item['descriptions'])
        )
        item_claims = json.loads(item['claims'])
        item_instanceof = [c['mainsnak']['datavalue']['id'] for c in item_claims.get("P31", [])]

        label_included = item_label and (item_label != '') # Exclude items with no label
        content_included = (item_description and (item_description != ''))\
                            or len(item_claims) > 0 # Exclude items with no claims or no description
        not_disambiguation = 'Q4167410' not in item_instanceof # Exclude disambiguation pages
        push_check = (not CHECK_IDS_PUSHED) or \
            (not WikidataIDLog.is_pushed(item_id))

        if label_included \
            and content_included \
                and not_disambiguation \
                    and push_check:

            entity_obj = SimpleNamespace()
            entity_obj.id = item_id
            entity_obj.label = json.loads(item['labels'])
            entity_obj.description = json.loads(item['descriptions'])
            entity_obj.aliases = json.loads(item['aliases'])
            entity_obj.claims = item_claims

            chunks = textifier.chunk_text(
                entity_obj,
                graph_store.tokenizer,
                max_length=graph_store.max_token_size
            )

            for chunk_i, chunk in enumerate(chunks):
                db_id = f"{item_id}_{LANGUAGE}_{chunk_i+1}"
                ID_name = "QID" if item_id.startswith('Q') else "PID"
                metadata = {
                    "Label": item_label,
                    "Description": item_description,
                    "Date": datetime.now().isoformat(),
                    ID_name: item_id,
                    "ChunkID": chunk_i + 1,
                    "Language": LANGUAGE,
                    "InstanceOf": item_instanceof,
                    "IsItem": item_id.startswith('Q'),
                    "IsProperty": item_id.startswith('P'),
                    "DumpDate": DUMPDATE
                }

                pushed_ids = graph_store.add_document(
                    id=db_id,
                    text=chunk,
                    metadata=metadata
                )
                if pushed_ids:
                    WikidataIDLog.add_ids(pushed_ids)


    pushed_ids = graph_store.push_all()
    if pushed_ids:
        WikidataIDLog.add_ids(pushed_ids)

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

        for _ in range(NUM_PROCESSES):
            queue.put(None)

        while any(p.is_alive() for p in processes):
            pbar.update(progress_bar.value - pbar.n)
            time.sleep(1)

        for p in processes:
            p.join()
