# # How to read and process the Wikdata dump file.

import sqlite3
from wikidataDumpReader import WikidataDumpReader
from multiprocessing import Manager, Queue, Process, cpu_count

def db_writer(db_path, queue):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    while True:
        item = queue.get()
        if item is None:
            break

        query, params = item
        cursor.execute(query, params)
        conn.commit()

    conn.close()

def setup_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikipedia_lang (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikidata_label_lang (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikidata_desc_lang (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikidata_lang (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikidata_lang_wikionly (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wikidata_wikipedia_lang (
            lang TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS claim_pid (
            pid TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS claim_pid_wikionly (
            pid TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS instance_of (
            qid TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS instance_of_wikionly (
            qid TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS item_type (
            type TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')

    conn.commit()
    return conn

def get_wikipedia_lang(entity):
    """
    Return the languages of all Wikipedia pages connected to the Wikidata entity.
    """
    langs = set()
    if ('sitelinks' in entity):
        for s in entity['sitelinks']:
            if s.endswith('wiki'):
                langs.add(s.split('wiki')[0])
    return langs

def get_wikidata_label_lang(entity):
    """
    Return the languages supported in this Wikidata entity (label and description)
    """
    wikidata_label_langs = set(entity.get('labels', {}).keys())
    return wikidata_label_langs

def get_wikidata_desc_lang(entity):
    """
    Return the languages supported in this Wikidata entity (label and description)
    """
    wikidata_desc_langs = set(entity.get('descriptions', {}).keys())
    return wikidata_desc_langs

def get_claims_pids(entity):
    """
    Return the list of properties connected to the Wikidata entity.
    """
    pids_count = {}
    for pid,claim in entity.get('claims', {}).items():
        pids_count[pid] = pids_count.get(pid, 0) +1

        for c in claim:
            if 'qualifiers' in c:
                for pid,_ in c['qualifiers'].items():
                    pids_count[pid] = pids_count.get(pid, 0) +1
    return pids_count

def get_instance_of(entity):
    """
    Return the instance of QID values
    """
    instance_of = set()
    if 'P31' in entity.get('claims', {}):
        for c in entity['claims']['P31']:
            if ('mainsnak' in c) and ('datavalue' in c['mainsnak']):
                if (c['mainsnak'].get('datatype', '') == 'wikibase-item'):
                    qid = c['mainsnak']['datavalue']['value']['id']
                    instance_of.add(qid)
    return instance_of


def calculate_stats(item, counters, queue=None):
    """
    Calculate various statistics for a given Wikidata item.
    """

    if item is not None:
        id_type = item['id'][0]
        wikipedia_langs = get_wikipedia_lang(item)

        wikidata_label_langs = get_wikidata_label_lang(item)
        wikidata_desc_langs = get_wikidata_desc_lang(item)
        wikidata_lbldesc_langs = wikidata_label_langs.intersection(wikidata_desc_langs)

        claims_pids = get_claims_pids(item)
        instance_of = get_instance_of(item)


        if queue is not None:
            queue.put((
                'INSERT OR IGNORE INTO item_type (type,count) VALUES (?,0)',
                (id_type,)
            ))
            queue.put((
                'UPDATE item_type SET count = count + 1 WHERE type = ?',
                (id_type,)
            ))

        item_type_count = counters['item_type']  # temp variable
        counters['item_type'][id_type] = item_type_count.get(id_type, 0) +1

        if id_type == 'Q':
            if queue is not None:
                update_sqldb(
                    wikipedia_langs,
                    wikidata_label_langs,
                    wikidata_desc_langs,
                    wikidata_lbldesc_langs,
                    claims_pids,
                    instance_of,
                    queue
                )

            update_counts(
                wikipedia_langs,
                wikidata_label_langs,
                wikidata_desc_langs,
                wikidata_lbldesc_langs,
                claims_pids,
                instance_of,
                counters
            )

def update_sqldb(
    wikipedia_langs: set, wikidata_label_langs: set, wikidata_desc_langs: set,
    wikidata_langs: set, claims_pids: dict, instance_of: set, 
    queue=None) -> None:

    for lang in wikipedia_langs:
        queue.put((
            'INSERT OR IGNORE INTO wikipedia_lang (lang, count) VALUES (?, 0)',
            (lang,)
        ))
        queue.put((
            'UPDATE wikipedia_lang SET count = count + 1 WHERE lang = ?',
            (lang,)
        ))

    for lang in wikidata_label_langs:
        queue.put((
            'INSERT OR IGNORE INTO wikidata_label_lang '
            '(lang, count) VALUES (?, 0)',
            (lang,)
        ))
        queue.put((
            'UPDATE wikidata_label_lang SET count = count + 1 WHERE lang = ?',
            (lang,)
        ))

    for lang in wikidata_desc_langs:
        queue.put((
            'INSERT OR IGNORE INTO wikidata_desc_lang (lang, count) '
            'VALUES (?, 0)',
            (lang,)
        ))
        queue.put((
            'UPDATE wikidata_desc_lang SET count = count + 1 WHERE lang = ?',
            (lang,)
        ))

    for lang in wikidata_langs:
        queue.put((
            'INSERT OR IGNORE INTO wikidata_lang (lang, count) VALUES (?, 0)',
            (lang,)
        ))

        queue.put((
            'UPDATE wikidata_lang SET count = count + 1 WHERE lang = ?',
            (lang,)
        ))

    for pid, count in claims_pids.items():
        queue.put((
            'INSERT OR IGNORE INTO claim_pid (pid, count) VALUES (?, 0)',
            (pid,)
        ))
        queue.put((
            'UPDATE claim_pid SET count = count + ? WHERE pid = ?',
            (count, pid,)
        ))

    for qid in instance_of:
        queue.put((
            'INSERT OR IGNORE INTO instance_of (qid, count) VALUES (?, 0)', 
            (qid,)
        ))
        queue.put((
            'UPDATE instance_of SET count = count + 1 WHERE qid = ?',
            (qid,)
        ))

    if len(wikipedia_langs) > 0:
        queue.put((
            'INSERT OR IGNORE INTO wikipedia_lang (lang, count) '
            'VALUES ("total", 0)',
            ()
        ))
        queue.put((
            'UPDATE wikipedia_lang SET count = count + 1 WHERE lang = "total"', ()
        ))

        for lang in wikidata_langs:
            queue.put((
                'INSERT OR IGNORE INTO wikidata_lang_wikionly '
                '(lang, count) VALUES (?, 0)',
                (lang,)
            ))
            queue.put((
                'UPDATE wikidata_lang_wikionly '
                'SET count = count + 1 '
                'WHERE lang = ?',
                (lang,)
            ))

        for pid, count in claims_pids.items():
            queue.put((
                'INSERT OR IGNORE INTO claim_pid_wikionly '
                '(pid, count) VALUES (?, 0)',
                (pid,)
            ))
            queue.put((
                'UPDATE claim_pid_wikionly '
                'SET count = count + ? '
                'WHERE pid = ?',
                (count, pid,)
            ))

        for qid in instance_of:
            queue.put((
                'INSERT OR IGNORE INTO instance_of_wikionly (qid, count) '
                'VALUES (?, 0)',
                (qid,)
            ))
            queue.put((
                'UPDATE instance_of_wikionly '
                'SET count = count + 1 '
                'WHERE qid = ?',
                (qid,)
            ))

    for lang in wikidata_langs.intersection(wikipedia_langs):
        queue.put((
            'INSERT OR IGNORE INTO wikidata_wikipedia_lang (lang, count) '
            'VALUES (?, 0)',
            (lang,)
        ))
        queue.put((
            'UPDATE wikidata_wikipedia_lang '
            'SET count = count + 1 '
            'WHERE lang = ?',
            (lang,)
        ))

def update_counts(
    wikipedia_langs: set, wikidata_label_langs: set, wikidata_desc_langs: set,
    wikidata_langs: set, claims_pids: dict, instance_of: set, 
    counters: dict, cursor=None) -> None:

    for lang in wikipedia_langs:
        counters['wikipedia_lang'][lang] = counters['wikipedia_lang'].get(lang, 0) +1

    for lang in wikidata_label_langs:
        counters['wikidata_label_lang'][lang] = counters['wikidata_label_lang'].get(lang, 0) +1

    for lang in wikidata_desc_langs:
        counters['wikidata_desc_lang'][lang] = counters['wikidata_desc_lang'].get(lang, 0) +1

    for lang in wikidata_langs:
        counters['wikidata_lang'][lang] = counters['wikidata_lang'].get(lang, 0) +1

    for pid, count in claims_pids.items():
        counters['claim_pid'][pid] = counters['claim_pid'].get(pid, 0) + count

    for qid in instance_of:
        counters['instance_of'][qid] = counters['instance_of'].get(qid, 0) +1

    if len(wikipedia_langs) > 0:
        counters['wikipedia_lang']['total']  = counters['wikipedia_lang'].get('total', 0) +1

        for lang in wikidata_langs:
            counters['wikidata_lang_wikionly'][lang] = counters['wikidata_lang_wikionly'].get(lang, 0) +1

        for pid, count in claims_pids.items():
            counters['claim_pid_wikionly'][pid] = counters['claim_pid_wikionly'].get(pid, 0) + count

        for qid in instance_of:
            counters['instance_of_wikionly'][qid] = counters['instance_of_wikionly'].get(qid, 0) +1

    for lang in wikidata_langs.intersection(wikipedia_langs):
        counters['wikidata_wikipedia_lang'][lang] = counters['wikidata_wikipedia_lang'].get(lang, 0) +1


if __name__ == '__main__':
    import os

    import sys
    sys.path.append('../src')

    from wikidataDumpReader import WikidataDumpReader
    from multiprocessing import Manager, cpu_count, Queue, Process

    from src.experimental_functions.Get_Wikidata_Stats import calculate_stats, setup_database, db_writer

    # Set up database
    DB_PATH = 'wikidata_stats.db'
    conn = setup_database(DB_PATH)

    # FILEPATH = '../data/Wikidata/latest-all.json.bz2'
    FILEDIR = '/mnt/wwn-0x5000c500f7e371c0-part1/WikidataUnplugged/'
    FILENAME = 'latest-all-Sept26.json' # .bz2'
    FILEPATH = os.path.join(FILEDIR, FILENAME)

    QUEUE_SIZE = 15000
    NUM_PROCESSES = cpu_count() - 1
    SKIPLINES = 0

    # Initialize multiprocessing manager
    multiprocess_manager = Manager()

    # Create a queue for database writes
    db_queue = Queue()

    # Start the database writer process
    db_writer_process = Process(target=db_writer, args=(DB_PATH, db_queue))
    db_writer_process.start()

    # Shared dictionaries for statistics
    counter_names = [
        # Per language, count the the items that are connected to the Wikipedia page of the language
        'wikipedia_lang',

        # Per language, count the the items that have a label supported in the language
        'wikidata_label_lang',

        # Per language, count the the items that have a description supported in the language
        'wikidata_desc_lang',

        # Per language, count the the items that have a label and description supported in the language
        'wikidata_lang',

        # Same as wikidatalang but for items that are connected to a Wikipedia page
        'wikidata_lang_wikionly',

        # The intersection of wikipedialang and wikidatalang
        'wikidata_wikipedia_lang',

        # Per claim, count how many times it's been included in an item
        'claim_pid',

        # Same as claim but for items that are connected to a Wikipedia page
        'claim_pid_wikionly',

        # Count the distinct values of instance of claim
        'instance_of',

        # Same as instance_of but for items that are connected to a Wikipedia page
        'instance_of_wikionly',

        # Number of QIDs vs PIDs vs LIDs...
        'item_type'
    ]
    counters = {cname:multiprocess_manager.dict() for cname in counter_names}

    """
    wikipedialang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a label supported in the language
    wikidatalabellang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a description 
    # supported in the language
    wikidatadesclang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a label and description 
    # supported in the language
    wikidatalang_counts = multiprocess_manager.dict() 

    # Same as wikidatalang_counts but for items that are connected 
    # to a Wikipedia page
    wikidatalang_counts_wikionly = multiprocess_manager.dict() 

    # The intersection of wikipedialang_counts and wikidatalang_counts.
    wikidata_wikipedia_lang_counts = multiprocess_manager.dict() 

    # Per claim, count how many times it's been included in an item
    claim_counts = multiprocess_manager.dict() 

    # Same as claim_counts but for items that are connected to a Wikipedia page
    claim_counts_wikionly = multiprocess_manager.dict() 

    # Count the distinct values of instance of claim
    instance_of_counts = multiprocess_manager.dict() 

    # Same as instance_of_counts but for items that are connected to a Wikipedia page
    instance_of_counts_wikionly = multiprocess_manager.dict()

    # Number of QIDs vs PIDs vs LIDs...
    item_type_count = multiprocess_manager.dict()
    """

    # Create a WikidataDumpReader object to process counters
    wikidata = WikidataDumpReader(
        FILEPATH,
        num_processes=NUM_PROCESSES,
        queue_size=QUEUE_SIZE,
        skiplines=SKIPLINES
    )

    wikidata.run(
        lambda item: calculate_stats(item, counters, db_queue),
        # max_iterations=10000,
        verbose=True
    )

    # Signal the database writer process to exit
    db_queue.put(None)
    db_writer_process.join()

    # Close the database connection
    conn.close()