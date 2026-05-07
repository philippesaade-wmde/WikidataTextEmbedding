from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import WikidataPropertyFilter, WikidataItemFilter
from src.WikidataJSONCleaner import WikidataJSONCleaner
from src.utils import chunk_item_text, extract_instanceof, extract_pids, check_wdtextifier_stack
from src.JinaAI import JinaAITokenizer, JinaAIAPIEmbedder
from src.wikidataVectorCache import WikidataVectorCache
from src.wikidataVectorDB import AstraDBConnect
from src.wikidataHuggingFace import WikidataHFDatasetPublisher

from WikidataTextifier.src import WikidataLabel, LazyLabelFactory, JSONNormalizer

import os
from datetime import datetime, timezone

READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", 128))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", 16))
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
LANG = os.environ.get("WD_LANG", os.environ.get("LANG", "en"))
FALLBACK_LANG = os.environ.get("FALLBACK_LANG", LANG)
WD_LANGS = tuple(lang.strip() for lang in os.environ.get("WD_LANGS", "").split(",") if lang.strip())

JINA_API_PATH = os.environ.get("JINA_API_PATH", "./API_tokens/jina_api.json")
ASTRA_API_PATH = os.environ.get("ASTRA_API_PATH", "./API_tokens/datastax_api.json")
WD_HF_API_PATH = os.environ.get("WD_HF_API_PATH", "./API_tokens/wd_hf_api.json")
VECTORS_HF_API_PATH = os.environ.get("VECTORS_HF_API_PATH", "./API_tokens/vectors_hf_api.json")
HF_CHUNK_SIZE = int(os.environ.get("HF_CHUNK_SIZE", 10000))
DUMP_DATE = os.environ.get("DUMP_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
HF_BRANCH = os.environ.get("HF_BRANCH", datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
VECTOR_HF_BRANCH = os.environ.get("VECTOR_HF_BRANCH", HF_BRANCH)
PROPERTY_CONSTRAINT_PIDS = tuple(
    pid.strip() for pid in os.environ.get("PROPERTY_CONSTRAINT_PIDS", "P2302").split(",") if pid.strip()
)

SAVE_WD_TO_HF = os.environ.get("SAVE_WD_TO_HF", "false").lower() == "true"
SAVE_VECTORS_TO_HF = os.environ.get("SAVE_VECTORS_TO_HF", "false").lower() == "true"
SAVE_TO_VECTORDB = os.environ.get("SAVE_TO_VECTORDB", "false").lower() == "true"
SAVE_LABELS = os.environ.get("SAVE_LABELS", "false").lower() == "true"
FORCE_DOWNLOAD_DUMP = os.environ.get("FORCE_DOWNLOAD_DUMP", "false").lower() == "true"

TEXT_PROPERTY_FILTER = None
TEXT_TOKENIZER = None
VECTOR_ITEM_FILTER = None
VECTOR_EMBEDDER = None
VECTORCACHE = None
ASTRADB = None
HF_PUBLISHER = None

NEEDS_DUMP_READER = SAVE_LABELS or SAVE_WD_TO_HF or SAVE_TO_VECTORDB

dump_reader = None


'''
Step 2: Read the dump and save labels of all entities
'''

def save_labels(items):
    data = {
        item["id"]: {"labels": item["labels"]}
        for item in items
    }
    if not data:
        return
    compressed = WikidataLabel._compress_labels(data)
    WikidataLabel.add_bulk_labels(
        [{"id": qid, "labels": labels} for qid, labels in compressed.items()]
    )

'''
Step 3: Read the dump and prepare for text embedding
'''

def item_to_json(item, label_factory=None):
    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    # Preparation for HuggingFace
    clean_json = WikidataJSONCleaner.clean_entity(
        item,
        label_factory.create
    )

    label_factory.resolve_all()
    clean_json = label_factory.resolve_labels_in_json(clean_json)
    return clean_json


def item_to_text(item, label_factory=None):
    global TEXT_PROPERTY_FILTER, TEXT_TOKENIZER

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    last_modified = item['modified']

    # Preparation for text embedding
    normalizer = JSONNormalizer(
        item['id'],
        item,
        lang=LANG,
        fallback_lang=FALLBACK_LANG,
        label_factory=label_factory
    )

    item = normalizer.normalize(
        external_ids=False,
        references=False,
        all_ranks=False,
        qualifiers=True
    )

    # Sort and filter claims based on the property classification
    if TEXT_PROPERTY_FILTER is None:
        TEXT_PROPERTY_FILTER = WikidataPropertyFilter()
    drop_claim_pids = PROPERTY_CONSTRAINT_PIDS if item.id.startswith("P") else ()
    item = TEXT_PROPERTY_FILTER.sort_and_filter_textifier(
        item,
        drop_claim_pids=drop_claim_pids,
    )

    # Resolve pending lazy labels once before repeated to_text() calls in chunking.
    label_factory.resolve_all()

    if TEXT_TOKENIZER is None:
        TEXT_TOKENIZER = JinaAITokenizer()
    chunks = chunk_item_text(
        item,
        TEXT_TOKENIZER,
        max_length=1024,
        lang=LANG
    )

    return [
        {
            '_id': f"{item.id}_{LANG}_{i+1}",
            'content': chunk,
            'metadata': {
                "Label": item.label,
                "Description": item.description,
                "QID" if item.id.startswith("Q") else "PID": item.id,
                "ChunkID": i + 1,
                "Language": LANG,
                "InstanceOf": extract_instanceof(item),
                "Properties": extract_pids(item),
                "LastModified": last_modified,
                "DumpDate": DUMP_DATE,
            }
        }
        for i, chunk in enumerate(chunks)
    ]

def push_to_hf(items, label_factory=None):
    if HF_PUBLISHER is None:
        raise RuntimeError(
            "HF publisher is not initialized in this process."
        )

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)
    items = [
        item_to_json(item, label_factory=label_factory) for item in items
    ]

    HF_PUBLISHER.publish_wd_batch(items)
    return len(items)


def save_vectors_to_hf():
    vector_cache = WikidataVectorCache(lang=LANG, data_dir="./data/Wikidata/")
    total = 0
    for vectors in vector_cache.iter_batches(batch_size=HF_CHUNK_SIZE):
        total += HF_PUBLISHER.publish_vector_batch(vectors)
    return total


def init_vector_worker():
    """
    Initialize process-local clients once per consumer process.
    """
    global VECTORCACHE, ASTRADB
    global VECTOR_ITEM_FILTER, VECTOR_EMBEDDER

    VECTOR_ITEM_FILTER = WikidataItemFilter(lang=LANG, fallback_lang=FALLBACK_LANG)
    VECTOR_EMBEDDER = JinaAIAPIEmbedder(config_path=JINA_API_PATH)
    VECTORCACHE = WikidataVectorCache(lang=LANG, data_dir="./data/Wikidata/")
    ASTRADB = AstraDBConnect(lang=LANG, config_path=ASTRA_API_PATH)


def push_to_vectorDB(items, label_factory=None):
    global VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB
    if any(x is None for x in (VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB)):
        init_vector_worker()

    # Filter items for embedding
    items = [
        item for item in items \
            if VECTOR_ITEM_FILTER.filter(item)
    ]

    # Split between items to create or to update
    to_update, to_create = VECTORCACHE.filter_for_update(items)

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)
    to_update_docs = []
    for item in to_update:
        item_chunks = item_to_text(item, label_factory=label_factory)
        to_update_docs.extend(item_chunks)

    to_create_docs = []
    for item in to_create:
        item_chunks = item_to_text(item, label_factory=label_factory)
        to_create_docs.extend(item_chunks)

    # Vectorize updates + creates together in one batch call
    all_docs = to_update_docs + to_create_docs
    if not all_docs:
        return 0

    vectors = VECTOR_EMBEDDER.embed_documents(
        [doc["content"] for doc in all_docs]
    )

    for doc, vector in zip(all_docs, vectors):
        doc["$vector"] = vector

    created_ids = ASTRADB.create_documents(to_create_docs)

    not_created_docs = [doc for doc in to_create_docs \
        if doc['_id'] not in created_ids]
    to_update_docs.extend(not_created_docs)
    updated_ids = ASTRADB.update_documents(to_update_docs)
    all_ids = set(created_ids) | set(updated_ids)

    # Cache docs
    to_cache = [doc for doc in all_docs \
        if doc['_id'] in all_ids]
    VECTORCACHE.add_astra_doc(to_cache)

    return len(all_ids)


def process_label_batch(items):
    save_labels(items)


def process_second_pass_batch(items):
    label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    if SAVE_WD_TO_HF:
        push_to_hf(items, label_factory=label_factory)

    if SAVE_TO_VECTORDB:
        push_to_vectorDB(items, label_factory=label_factory)


def run_pipeline(
    run_first_pass_labels=SAVE_LABELS,
    run_second_pass_processing=(SAVE_WD_TO_HF or SAVE_TO_VECTORDB),
    run_vectors_to_hf=SAVE_VECTORS_TO_HF,
):
    global dump_reader, HF_PUBLISHER

    needs_dump_reader = run_first_pass_labels or run_second_pass_processing
    if needs_dump_reader:
        check_wdtextifier_stack()
        dump_reader = WikidataDumpReader(
            DUMP_PATH,
            queue_size=READER_QUEUE_SIZE,
            batch_size=READER_BATCH_SIZE,
        )

        # Step 1: Download the latest Wikidata dump
        if FORCE_DOWNLOAD_DUMP or (not os.path.exists(DUMP_PATH)):
            print(f"File {DUMP_PATH} does not exist. Downloading...")
            dump_reader.download()
    else:
        dump_reader = None

    if run_first_pass_labels or run_second_pass_processing:
        WikidataLabel.initialize_database()

    if run_second_pass_processing and SAVE_WD_TO_HF:
        HF_PUBLISHER = WikidataHFDatasetPublisher(
            branch=HF_BRANCH,
            config_path=WD_HF_API_PATH,
            chunk_size=HF_CHUNK_SIZE
        )

    if run_first_pass_labels:
        dump_reader.run(
            process_label_batch,
            handler_receives_batch=True,
        )

    if run_second_pass_processing:
        try:
            dump_reader.run(
                process_second_pass_batch,
                handler_receives_batch=True,
                init_consumer=init_vector_worker if SAVE_TO_VECTORDB else None
            )
        finally:
            if SAVE_WD_TO_HF and HF_PUBLISHER is not None:
                HF_PUBLISHER.flush()

    if run_vectors_to_hf:
        HF_PUBLISHER = WikidataHFDatasetPublisher(
            branch=VECTOR_HF_BRANCH,
            config_path=VECTORS_HF_API_PATH,
            chunk_size=HF_CHUNK_SIZE,
            data_dir=f"data/{LANG}",
        )

        try:
            save_vectors_to_hf()
        finally:
            HF_PUBLISHER.flush()


def reset_runtime_state():
    global dump_reader, HF_PUBLISHER
    global TEXT_PROPERTY_FILTER, TEXT_TOKENIZER
    global VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB

    dump_reader = None
    HF_PUBLISHER = None
    TEXT_PROPERTY_FILTER = None
    TEXT_TOKENIZER = None
    VECTOR_ITEM_FILTER = None
    VECTOR_EMBEDDER = None
    VECTORCACHE = None
    ASTRADB = None


def run_pipeline_all_languages():
    global LANG, FALLBACK_LANG, HF_BRANCH, VECTOR_HF_BRANCH

    if not WD_LANGS:
        run_pipeline()
        return

    if SAVE_LABELS:
        print("Running shared label pass once before per-language second passes")
        reset_runtime_state()
        run_pipeline(
            run_first_pass_labels=True,
            run_second_pass_processing=False,
            run_vectors_to_hf=False,
        )

    default_fallback = os.environ.get("FALLBACK_LANG")
    base_hf_branch = HF_BRANCH
    base_vector_hf_branch = VECTOR_HF_BRANCH
    for lang in WD_LANGS:
        fallback = os.environ.get(f"FALLBACK_LANG_{lang.upper()}", default_fallback or lang)
        print(f"Running pipeline for language={lang} (fallback={fallback})")
        LANG = lang
        FALLBACK_LANG = fallback
        HF_BRANCH = f"{base_hf_branch}-{lang}"
        VECTOR_HF_BRANCH = f"{base_vector_hf_branch}-{lang}"
        reset_runtime_state()
        run_pipeline(
            run_first_pass_labels=False,
            run_second_pass_processing=(SAVE_WD_TO_HF or SAVE_TO_VECTORDB),
            run_vectors_to_hf=SAVE_VECTORS_TO_HF,
        )


if __name__ == "__main__":
    run_pipeline_all_languages()
