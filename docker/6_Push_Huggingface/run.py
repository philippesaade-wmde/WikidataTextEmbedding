import os
import json
from huggingface_hub import login
from multiprocessing import Process, Value, Queue


from src.wikidataDumpReader import WikidataDumpReader
from src.wikidataItemDB import WikidataItem

from datasets import Dataset, load_dataset_builder

# Constants
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 5000))
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
SKIPLINES = int(os.getenv("SKIPLINES", 0))
API_KEY_FILENAME = os.getenv("API_KEY", "huggingface_api.json")
ITERATION = int(os.getenv("ITERATION", 0))

api_key_fpath = f"../API_tokens/{API_KEY_FILENAME}"
with open(api_key_fpath) as f_in:
    api_key = json.load(open(f_in))['API_KEY']


def save_to_queue(item, data_queue):
    """Processes and puts cleaned item into the multiprocessing queue."""
    if (item is not None) and (WikidataItem.is_in_wikipedia(item)):
        claims = WikidataItem.add_labels_batched(
            item['claims'],
            query_batch=100
        )
        data_queue.put({
            'id': item['id'],
            'labels': json.dumps(item['labels'], separators=(',', ':')),
            'descriptions': json.dumps(
                item['descriptions'],
                separators=(',', ':')
            ),
            'aliases': json.dumps(item['aliases'], separators=(',', ':')),
            'sitelinks': json.dumps(item['sitelinks'], separators=(',', ':')),
            'claims': json.dumps(claims, separators=(',', ':'))
        })


def chunk_generator(filepath, num_processes=2, queue_size=5000, skip_lines=0):
    """
    A generator function that reads a chunk file with WikidataDumpReader,
    processes each item, and yields the result. It uses a multiprocessing
    queue to handle data ingestion in parallel without storing everything
    in memory.
    """
    data_queue = Queue(maxsize=queue_size)
    finished = Value('i', 0)

    # Initialize the dump reader
    wikidata = WikidataDumpReader(
        filepath,
        num_processes=num_processes,
        queue_size=queue_size,
        skiplines=skip_lines
    )

    # Define a function to feed items into the queue
    def run_reader():
        wikidata.run(
            lambda item: save_to_queue(item, data_queue),
            max_iterations=None,
            verbose=True
        )

        with finished.get_lock():
            finished.value = 1

    # Start reader in a separate process
    reader_proc = Process(target=run_reader)
    reader_proc.start()

    # Continuously yield items from the queue to the Dataset generator
    while True:
        # If reader is done AND queue is empty => stop
        if finished.value == 1 and data_queue.empty():
            break
        try:
            item = data_queue.get(timeout=1)
        except Exception as e:
            print(f'Exception: {e}')
            continue
        if item:
            yield item

    # Wait for the reader process to exit
    reader_proc.join()


if __name__ == "__main__":
    # TODO: Convert the following into a function and run it here
    # Now process each chunk file and push to the same Hugging Face repo
    HF_REPO_ID = "wikidata"  # Change to your actual repo on Hugging Face

    login(token=api_key)
    builder = load_dataset_builder("philippesaade/wikidata")
    for i in range(0, 113):
        split_name = f"chunk_{i}"
        if split_name not in builder.info.splits:
            filepath = f"../data/Wikidata/latest-all-chunks/chunk_{i}.json.gz"

            print(f"Processing {filepath} -> split={split_name}")

            # Create a Dataset from the generator
            ds_chunk = Dataset.from_generator(lambda: chunk_generator(
                filepath,
                num_processes=NUM_PROCESSES,
                queue_size=QUEUE_SIZE,
                skip_lines=SKIPLINES
            ))

            # Push each chunk as a separate "split" under the same dataset repo
            ds_chunk.push_to_hub(HF_REPO_ID, split=split_name)
            print(f"Chunk {ITERATION} pushed to {HF_REPO_ID} as {split_name}.")
