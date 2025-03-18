from multiprocessing import Manager
import os
import time

from src.wikidataDumpReader import WikidataDumpReader
from src.wikidataLangDB import WikidataLang

FILEPATH = os.getenv("FILEPATH", '../data/Wikidata/latest-all.json.bz2')
PUSH_SIZE = int(os.getenv("PUSH_SIZE", 2000))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 1500))
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 8))
SKIPLINES = int(os.getenv("SKIPLINES", 0))
LANGUAGE = os.getenv("LANGUAGE", 'en')


def save_entities_to_sqlite(item, data_batch, sqliteDBlock):
    """_summary_
    # TODO Add a docstring

    Args:
        item (_type_): _description_
        data_batch (_type_): _description_
        sqliteDBlock (_type_): _description_
    """
    if item is not None:
        # Check if the item is a valid entity
        return

    lang_in_wp = WikidataLang.is_in_wikipedia(item, language=LANGUAGE)
    if not lang_in_wp:
        # If the entity is not in the specified language Wikipedia, skip
        return

    item = WikidataLang.normalise_item(item, language=LANGUAGE)
    data_batch.append(item)

    with sqliteDBlock:
        if len(data_batch) > PUSH_SIZE:
            worked = WikidataLang.add_bulk_entities(list(
                data_batch[:PUSH_SIZE]
            ))
            if worked:
                del data_batch[:PUSH_SIZE]


if __name__ == "__main__":
    multiprocess_manager = Manager()
    sqliteDBlock = multiprocess_manager.Lock()
    data_batch = multiprocess_manager.list()

    wikidata = WikidataDumpReader(
        FILEPATH,
        num_processes=NUM_PROCESSES,
        queue_size=QUEUE_SIZE,
        skiplines=SKIPLINES
    )

    wikidata.run(
        lambda item: save_entities_to_sqlite(
            item,
            data_batch,
            sqliteDBlock
        ),
        max_iterations=None,
        verbose=True
    )

    while len(data_batch) > 0:
        worked = WikidataLang.add_bulk_entities(list(data_batch))
        if worked:
            del data_batch[:PUSH_SIZE]
        else:
            time.sleep(1)
