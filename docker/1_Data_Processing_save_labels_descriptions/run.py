from multiprocessing import Manager
import os
import time
import json

from src.wikidataDumpReader import WikidataDumpReader
from src.wikidataItemDB import WikidataItem

FILEPATH = os.getenv("FILEPATH", '../data/Wikidata/latest-all.json.bz2')
PUSH_SIZE = int(os.getenv("PUSH_SIZE", 20000))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 15000))
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
SKIPLINES = int(os.getenv("SKIPLINES", 0))
LANGUAGE = os.getenv("LANGUAGE", 'en')


def save_items_to_sqlite(item, data_batch, sqlitDBlock):
    if (item is not None):
        labels = WikidataItem.clean_label_description(item['labels'])
        descriptions = WikidataItem.clean_label_description(
            item['descriptions']
        )
        labels = json.dumps(labels, separators=(',', ':'))
        descriptions = json.dumps(descriptions, separators=(',', ':'))
        in_wikipedia = WikidataItem.is_in_wikipedia(item)
        data_batch.append({
            'id': item['id'],
            'labels': labels,
            'descriptions': descriptions,
            'in_wikipedia': in_wikipedia,
        })

        with sqlitDBlock:
            if len(data_batch) > PUSH_SIZE:
                worked = WikidataItem.add_bulk_items(list(
                    data_batch[:PUSH_SIZE]
                ))
                if worked:
                    del data_batch[:PUSH_SIZE]


if __name__ == "__main__":
    multiprocess_manager = Manager()
    sqlitDBlock = multiprocess_manager.Lock()
    data_batch = multiprocess_manager.list()

    wikidata = WikidataDumpReader(
        FILEPATH,
        num_processes=NUM_PROCESSES,
        queue_size=QUEUE_SIZE,
        skiplines=SKIPLINES
    )

    wikidata.run(
        lambda item: save_items_to_sqlite(
            item,
            data_batch,
            sqlitDBlock
        ),
        max_iterations=None,
        verbose=True
    )

    while len(data_batch) > 0:
        worked = WikidataItem.add_bulk_items(list(data_batch))
        if worked:
            del data_batch[:PUSH_SIZE]
        else:
            time.sleep(1)
