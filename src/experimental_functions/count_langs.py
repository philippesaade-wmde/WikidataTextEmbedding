import json
import os
import hashlib
import time

from datasets import load_dataset
from datetime import datetime
from multiprocessing import Process, Queue, Manager
from tqdm import tqdm

dataset = load_dataset(
    "philippesaade/wikidata",
    streaming=True,
    split="train"
)

QUEUE_SIZE = 5000
total_entities = 30_072_707
NUM_PROCESSES = 8

def process_items(queue, progress_bar, label_counts, sitelink_counts, intersection_counts):
    """Worker function that processes items from the queue
        and adds them to AstraDB.
    """
    while True:
        item = queue.get()
        progress_bar.value += 1
        if item is None:
            # Exit condition for worker processes
            break

        item_labels = json.loads(item['labels'])
        item_sitelinks = json.loads(item['sitelinks'])

        label_langs = set()
        sitelink_langs = set()

        # Count label languages
        for lang in item_labels:
            label_langs.add(lang)
            with label_counts_lock:
                label_counts[lang] = label_counts.get(lang, 0) + 1

        # Count sitelinks ending in 'wiki'
        for site in item_sitelinks:
            if site.endswith("wiki"):
                sitelink_lang = site.replace("wiki", "")
                sitelink_langs.add(sitelink_lang)
                with sitelink_counts_lock:
                    sitelink_counts[site] = sitelink_counts.get(site, 0) + 1

        # Count intersection of languages
        intersection = label_langs.intersection(sitelink_langs)
        for lang in intersection:
            with intersection_counts_lock:
                intersection_counts[lang] = intersection_counts.get(lang, 0) + 1

if __name__ == "__main__":
    queue = Queue(maxsize=QUEUE_SIZE)
    manager = Manager()
    progress_bar = manager.Value("i", 0)

    label_counts = manager.dict()
    sitelink_counts = manager.dict()
    intersection_counts = manager.dict()

    # Locks to prevent race conditions when updating shared dicts
    global label_counts_lock, sitelink_counts_lock, intersection_counts_lock
    label_counts_lock = manager.Lock()
    sitelink_counts_lock = manager.Lock()
    intersection_counts_lock = manager.Lock()

    with tqdm(total=total_entities) as pbar:
        processes = []
        for _ in range(NUM_PROCESSES):
            p = Process(target=process_items, args=(queue, progress_bar, label_counts, sitelink_counts, intersection_counts))
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

    # Convert manager dicts to regular dicts
    final_data = {
        "label_counts": dict(label_counts),
        "sitelink_counts": dict(sitelink_counts),
        "intersection_counts": dict(intersection_counts),
    }

    # Save results to file
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = f"wikidata_language_stats_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)

    print(f"Language stats saved to: {output_file}")