import sys
# sys.path.append('../src')
sys.path.append('.')

import asyncio
import json
import psutil

from wikidata_dumpreader import WikidataDumpReader
from wikidataDB import Session, WikidataID, WikidataEntity
from multiprocessing import Lock

from sqlalchemy import select
from tqdm import tqdm



def in_en_wiki(item):
    return ('sitelinks' in item) and (f'{language}wiki' in item['sitelinks']) and ((language in item['labels']) or ('mul' in item['labels'])) and ((language in item['descriptions']) or ('mul' in item['descriptions']))

# bulk_ids = []
def count_types(item):
    global bulk_ids

    if item is not None:
        if in_en_wiki(item):
            bulk_ids.append({'id': item['id'], 'in_wikipedia': True, 'is_property': False})

            for pid,claim in item.get('claims', {}).items():
                bulk_ids.append({'id': pid, 'in_wikipedia': False, 'is_property': True})

                for c in claim:
                    if ('mainsnak' in c) and ('datavalue' in c['mainsnak']):
                        if (c['mainsnak'].get('datatype', '') == 'wikibase-item'):
                            id = c['mainsnak']['datavalue']['value']['id']
                            bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                        elif (c['mainsnak'].get('datatype', '') == 'wikibase-property'):
                            id = c['mainsnak']['datavalue']['value']['id']
                            bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': True})

                        elif (c['mainsnak'].get('datatype', '') == 'quantity') and (c['mainsnak']['datavalue']['value'].get('unit', '1') != '1'):
                            id = c['mainsnak']['datavalue']['value']['unit'].rsplit('/', 1)[1]
                            bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                    if 'qualifiers' in c:
                        for pid, qualifier in c['qualifiers'].items():
                            bulk_ids.append({'id': pid, 'in_wikipedia': False, 'is_property': True})
                            for q in qualifier:
                                if ('datavalue' in q):
                                    if (q['datatype'] == 'wikibase-item'):
                                        id = q['datavalue']['value']['id']
                                        bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                                    elif(q['datatype'] == 'wikibase-property'):
                                        id = q['datavalue']['value']['id']
                                        bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': True})

                                    elif (q['datatype'] == 'quantity') and (q['datavalue']['value'].get('unit', '1') != '1'):
                                        id = q['datavalue']['value']['unit'].rsplit('/', 1)[1]
                                        bulk_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

            with sqlitDBlock:
                if len(bulk_ids) > BATCH_SIZE:
                    worked = WikidataID.add_bulk_ids(bulk_ids)
                    if worked:
                        bulk_ids = []

                if len(bulk_ids) > 0:
                    worked = WikidataID.add_bulk_ids(bulk_ids)


def remove_keys(data, keys_to_remove=['hash', 'property', 'numeric-id', 'qualifiers-order']):
    if isinstance(data, dict):
        return {
            key: remove_keys(value, keys_to_remove)
            for key, value in data.items() if key not in keys_to_remove
        }
    elif isinstance(data, list):
        return [remove_keys(item, keys_to_remove) for item in data]
    else:
        return data

def get_claims(item):
    claims = {}
    if 'claims' in item:
        for pid,x in item['claims'].items():
            pid_claims = []
            for i in x:
                if (i['type'] == 'statement') and (i['rank'] != 'deprecated'):
                    pid_claims.append({
                        'mainsnak': remove_keys(i['mainsnak']) if 'mainsnak' in i else {},
                        'qualifiers': remove_keys(i['qualifiers']) if 'qualifiers' in i else {},
                        'rank': i['rank']
                    })
            if len(pid_claims) > 0:
                claims[pid] = pid_claims
    return claims

def get_aliases(item):
    aliases = set()
    if language in item['aliases']:
        aliases = set([x['value'] for x in item['aliases'][language]])
    if 'mul' in item['aliases']:
        aliases = aliases | set([x['value'] for x in item['aliases']['mul']])
    return list(aliases)

def save_entites_to_sqlite(item):
    global data_batch

    if item is not None:
        if WikidataID.get_id(item['id']):
            label = item['labels'][language]['value'] if (language in item['labels']) else (item['labels']['mul']['value'] if ('mul' in item['labels']) else '')
            description = item['descriptions'][language]['value'] if (language in item['descriptions']) else (item['descriptions']['mul']['value'] if ('mul' in item['descriptions']) else '')
            aliases = get_aliases(item)
            claims = get_claims(item)
            data_batch.append({
                'id': item['id'],
                'label': label,
                'description': description,
                'aliases': json.dumps(aliases, separators=(',', ':')),
                'claims': json.dumps(claims, separators=(',', ':')),
            })
            progressbar.update(1)

            process = psutil.Process()
            progressbar.set_description(f"Batch Size: {len(data_batch)} \t Memory Usage: {process.memory_info().rss / 1024 ** 2:.2f} MB")
            with sqlitDBlock:
                if len(data_batch) >= BATCH_SIZE:
                    worked = WikidataEntity.add_bulk_entities(data_batch)
                    if worked:
                        data_batch = []
                #     break

                # if len(data_batch) >= 0:
                #     worked = WikidataEntity.add_bulk_entities(data_batch)


def in_mul_and_not_en(item):
    return ('sitelinks' in item) and (f'{language}wiki' in item['sitelinks']) and (((language not in item['labels']) and ('mul' in item['labels'])) or ((language not in item['descriptions']) and ('mul' in item['descriptions'])))

def remove_keys(data, keys_to_remove=['hash', 'property', 'numeric-id', 'qualifiers-order']):
    if isinstance(data, dict):
        return {
            key: remove_keys(value, keys_to_remove) 
            for key, value in data.items() if key not in keys_to_remove
        }
    elif isinstance(data, list):
        return [remove_keys(item, keys_to_remove) for item in data]
    else:
        return data

def get_claims(item):
    claims = {}
    if 'claims' in item:
        for pid,x in item['claims'].items():
            pid_claims = []
            for i in x:
                if (i['type'] == 'statement') and (i['rank'] != 'deprecated'):
                    pid_claims.append({
                        'mainsnak': remove_keys(i['mainsnak']) if 'mainsnak' in i else {},
                        'qualifiers': remove_keys(i['qualifiers']) if 'qualifiers' in i else {},
                        'rank': i['rank']
                    })
            if len(pid_claims) > 0:
                claims[pid] = pid_claims
    return claims

def get_aliases(item):
    aliases = set()
    if language in item['aliases']:
        aliases = set([x['value'] for x in item['aliases'][language]])
    if 'mul' in item['aliases']:
        aliases = aliases | set([x['value'] for x in item['aliases']['mul']])
    return list(aliases)

# data_batch = []
# progressbar = tqdm(total=112473858)
# sqlitDBlock = Lock()
# language = 'en'
def save_entites_to_sqlite(item):
    global data_batch
    global missing_ids
    global IDtypes

    progressbar.update(1)
    if item is not None:
        if (item['id'] in missing_ids):
            label = item['labels'][language]['value'] if (language in item['labels']) else (item['labels']['mul']['value'] if ('mul' in item['labels']) else '')
            description = item['descriptions'][language]['value'] if (language in item['descriptions']) else (item['descriptions']['mul']['value'] if ('mul' in item['descriptions']) else '')
            aliases = get_aliases(item)
            claims = get_claims(item)
            data_batch.append({
                'id': item['id'],
                'label': label,
                'description': description,
                'aliases': json.dumps(aliases, separators=(',', ':')),
                'claims': json.dumps(claims, separators=(',', ':')),
            })

            process = psutil.Process()
            progressbar.set_description(f"Batch Size: {len(data_batch)} \t Memory Usage: {process.memory_info().rss / 1024 ** 2:.2f} MB")
            with sqlitDBlock:
                if len(data_batch) >= 1:
                    worked = WikidataEntity.add_bulk_entities(data_batch)
                    if worked:
                        data_batch = []


def get_missing_entities(session, ids):
    existing_entities = session.query(WikidataEntity.id).filter(WikidataEntity.id.in_(ids)).all()
    existing_ids = {entity.id for entity in existing_entities}
    return set(ids) - existing_ids


if __name__ == '__main__':

    FILEPATH = '../data/Wikidata/latest-all.json.bz2'
    BATCH_SIZE = 1000
    NUM_PROCESSES = 4  # or 6?
    language = 'en'
    sqlitDBlock = Lock()

    # data_batch = []
    # progressbar = tqdm(total=112473858)
    # sqlitDBlock = Lock()
    # language = 'en'
    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, skiplines=0)
    bulk_ids = []
    # Reading the Wikidata dump ZIP file and saving the IDs of entities and properties to a JSON file (Only the ones connected to the English Wikipedia)
    async def run_processor(wikidata):
        await wikidata.run(count_types, max_iterations=None, verbose=True)

    asyncio.run(run_processor(wikidata))
    """
    # Adding entities (label, description, claims, and aliases) of IDs found in WikidataID to WikidataEntity
    skiplines = 0
    data_batch = []
    progressbar = tqdm(total=12327824, desc="Running...")
    progressbar.update(skiplines)
    sqlitDBlock = Lock()
    language = 'en'


    async def run_processor(wikidata):
        await wikidata.run(save_entites_to_sqlite, max_iterations=None, verbose=False)

    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, skiplines=0)
    run_processor(wikidata)
    """
    """
    progressbar.close()
    if len(data_batch) > 0:
        WikidataEntity.add_bulk_entities(data_batch)

    # TODO: Name this section
    FILEPATH = '../data/Wikidata/latest-all.json.bz2'
    BATCH_SIZE = 10000
    NUM_PROCESSES = 4
    skiplines = 0
    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, skiplines=skiplines)

    async def run_processor():
        await wikidata.run(save_entites_to_sqlite, max_iterations=None, verbose=False)

    await run_processor()

    progressbar.close()
    if len(data_batch) > 0:
        WikidataEntity.add_bulk_entities(data_batch)

    # Find IDs that are in WikidataID but not in WikidataEntity
    with Session() as session:
        result = session.execute(
            select(WikidataID.id)
            .outerjoin(WikidataEntity, WikidataID.id == WikidataEntity.id)
            .filter(WikidataEntity.id == None)
            .filter(WikidataID.in_wikipedia == True)
        )
        missing_ids = set(result.scalars().all())

    print(len(missing_ids))

    # Find IDs that are not in WikidataEntity but are in the claims, qualifiers, and quantity units of entities connected to Wikipedia
    with Session() as session:
        entities = session.query(WikidataEntity).join(WikidataID, WikidataEntity.id == WikidataID.id).filter(WikidataID.in_wikipedia == True).yield_per(100000)

        progressbar = tqdm(total=9203531)
        found = False
        missing_ids = set()

        batch_size = 10000
        ids_to_check = set()

        for entity in entities:
            progressbar.update(1)
            for pid, claim in entity.claims.items():
                ids_to_check.add(pid)
                for c in claim:
                    if ('datavalue' in c['mainsnak']):
                        if ((c['mainsnak']['datatype'] == 'wikibase-item') or (c['mainsnak']['datatype'] == 'wikibase-property')):
                            id = c['mainsnak']['datavalue']['value']['id']
                            ids_to_check.add(id)
                        elif (c['mainsnak']['datatype'] == 'quantity') and (c['mainsnak']['datavalue']['value']['unit'] != '1'):
                            id = c['mainsnak']['datavalue']['value']['unit'].rsplit('/', 1)[1]
                            ids_to_check.add(id)

                    if 'qualifiers' in c:
                        for pid, qualifier in c['qualifiers'].items():
                            ids_to_check.add(pid)
                            for q in qualifier:
                                if ('datavalue' in q):
                                    if ((q['datatype'] == 'wikibase-item') or (q['datatype'] == 'wikibase-property')):
                                        id = q['datavalue']['value']['id']
                                        ids_to_check.add(id)
                                    elif (q['datatype'] == 'quantity') and (q['datavalue']['value']['unit'] != '1'):
                                        id = q['datavalue']['value']['unit'].rsplit('/', 1)[1]
                                        ids_to_check.add(id)

            if len(ids_to_check) >= batch_size:
                missing_ids.update(get_missing_entities(session, ids_to_check))
                ids_to_check.clear()

            if progressbar.n % 1000 == 0:
                progressbar.set_description(f"Missing IDs: {len(missing_ids)}")

        if ids_to_check:
            missing_ids.update(get_missing_entities(session, ids_to_check))

        progressbar.close()
    """