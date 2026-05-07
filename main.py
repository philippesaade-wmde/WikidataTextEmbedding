from datetime import datetime, timezone
import os

from WikidataTextifier.src import JSONNormalizer, LazyLabelFactory, WikidataLabel

from src.JinaAI import JinaAIAPIEmbedder, JinaAITokenizer
from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import WikidataItemFilter, WikidataPropertyFilter
from src.WikidataJSONCleaner import WikidataJSONCleaner
from src.utils import (
    check_wdtextifier_stack,
    chunk_item_text,
    extract_instanceof,
    extract_pids,
)
from src.wikidataHuggingFace import WikidataHFDatasetPublisher
from src.wikidataVectorCache import WikidataVectorCache
from src.wikidataVectorDB import AstraDBConnect


# ---- Runtime config ----
READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", 128))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", 16))
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", 4))
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
LANG = os.environ.get("WD_LANG", os.environ.get("LANG", "en"))
FALLBACK_LANG = os.environ.get("FALLBACK_LANG", LANG)
WD_LANGS = tuple(lang.strip() for lang in os.environ.get("WD_LANGS", "").split(",") if lang.strip())

JINA_API_PATH = os.environ.get("JINA_API_PATH", "./API_tokens/jina_api.json")
ASTRA_API_PATH = os.environ.get("ASTRA_API_PATH", "./API_tokens/datastax_api.json")
WD_HF_API_PATH = os.environ.get("WD_HF_API_PATH", "./API_tokens/wd_hf_api.json")
VECTORS_HF_API_PATH = os.environ.get("VECTORS_HF_API_PATH", "./API_tokens/vectors_hf_api.json")
HF_CHUNK_SIZE = int(os.environ.get("HF_CHUNK_SIZE", 1000))
HF_BATCH_SIZE = int(os.environ.get("HF_BATCH_SIZE", 32))
HF_QUEUE_SIZE = int(os.environ.get("HF_QUEUE_SIZE", 128))
DUMP_DATE = os.environ.get("DUMP_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
HF_BRANCH = os.environ.get("HF_BRANCH", datetime.now(timezone.utc).strftime("%Y%m%d"))
VECTOR_HF_BRANCH = os.environ.get("VECTOR_HF_BRANCH", HF_BRANCH)
PROPERTY_CONSTRAINT_PIDS = tuple(
    pid.strip() for pid in os.environ.get("PROPERTY_CONSTRAINT_PIDS", "P2302").split(",") if pid.strip()
)

SAVE_WD_TO_HF = os.environ.get("SAVE_WD_TO_HF", "false").lower() == "true"
SAVE_VECTORS_TO_HF = os.environ.get("SAVE_VECTORS_TO_HF", "false").lower() == "true"
SAVE_TO_VECTORDB = os.environ.get("SAVE_TO_VECTORDB", "false").lower() == "true"
SAVE_LABELS = os.environ.get("SAVE_LABELS", "false").lower() == "true"
FORCE_DOWNLOAD_DUMP = os.environ.get("FORCE_DOWNLOAD_DUMP", "false").lower() == "true"


# ---- Process-local runtime state ----
TEXT_PROPERTY_FILTER = None
TEXT_TOKENIZER = None
VECTOR_ITEM_FILTER = None
VECTOR_EMBEDDER = None
VECTORCACHE = None
ASTRADB = None
HF_PUBLISHER = None
LABEL_DB_READY = False
dump_reader = None


# ---- Transformation steps ----
def save_labels(items):
    data = {item["id"]: {"labels": item["labels"]} for item in items}
    if not data:
        return
    compressed = WikidataLabel._compress_labels(data)
    WikidataLabel.add_bulk_labels([{"id": qid, "labels": labels} for qid, labels in compressed.items()])


def item_to_json(item, label_factory=None):
    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    clean_json = WikidataJSONCleaner.clean_entity(item, label_factory.create)
    label_factory.resolve_all()
    return label_factory.resolve_labels_in_json(clean_json)


def item_to_text(item, label_factory=None):
    global TEXT_PROPERTY_FILTER, TEXT_TOKENIZER

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    last_modified = item.get("modified")

    normalizer = JSONNormalizer(
        item["id"],
        item,
        lang=LANG,
        fallback_lang=FALLBACK_LANG,
        label_factory=label_factory,
    )
    item = normalizer.normalize(
        external_ids=False,
        references=False,
        all_ranks=False,
        qualifiers=True,
    )

    if TEXT_PROPERTY_FILTER is None:
        TEXT_PROPERTY_FILTER = WikidataPropertyFilter()
    drop_claim_pids = PROPERTY_CONSTRAINT_PIDS if item.id.startswith("P") else ()
    item = TEXT_PROPERTY_FILTER.sort_and_filter_textifier(item, drop_claim_pids=drop_claim_pids)

    label_factory.resolve_all()

    if TEXT_TOKENIZER is None:
        TEXT_TOKENIZER = JinaAITokenizer()
    chunks = chunk_item_text(item, TEXT_TOKENIZER, max_length=1024, lang=LANG)

    return [
        {
            "_id": f"{item.id}_{LANG}_{i + 1}",
            "content": chunk,
            "metadata": {
                "Label": item.label,
                "Description": item.description,
                "QID" if item.id.startswith("Q") else "PID": item.id,
                "ChunkID": i + 1,
                "Language": LANG,
                "InstanceOf": extract_instanceof(item),
                "Properties": extract_pids(item),
                "LastModified": last_modified,
                "DumpDate": DUMP_DATE,
            },
        }
        for i, chunk in enumerate(chunks)
    ]


# ---- Sink steps ----
def push_to_hf(items, label_factory=None):
    if HF_PUBLISHER is None:
        raise RuntimeError("HF publisher is not initialized in this process.")

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    rows = [item_to_json(item, label_factory=label_factory) for item in items]
    HF_PUBLISHER.publish_wd_batch(rows)
    return len(rows)


def save_vectors_to_hf():
    if HF_PUBLISHER is None:
        raise RuntimeError("HF publisher is not initialized in this process.")

    vector_cache = WikidataVectorCache(lang=LANG, data_dir="./data/Wikidata/")
    total = 0
    for vectors in vector_cache.iter_batches(batch_size=HF_CHUNK_SIZE):
        total += HF_PUBLISHER.publish_vector_batch(vectors)
    return total


def push_to_vectorDB(items, label_factory=None):
    global VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB
    if any(x is None for x in (VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB)):
        init_worker(enable_vector=True)

    items = [item for item in items if VECTOR_ITEM_FILTER.filter(item)]
    to_update, to_create = VECTORCACHE.filter_for_update(items)

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    to_update_docs = []
    for item in to_update:
        to_update_docs.extend(item_to_text(item, label_factory=label_factory))

    to_create_docs = []
    for item in to_create:
        to_create_docs.extend(item_to_text(item, label_factory=label_factory))

    all_docs = to_update_docs + to_create_docs
    if not all_docs:
        return 0

    vectors = VECTOR_EMBEDDER.embed_documents([doc["content"] for doc in all_docs])
    for doc, vector in zip(all_docs, vectors):
        doc["$vector"] = vector

    created_ids = ASTRADB.create_documents(to_create_docs)
    not_created_docs = [doc for doc in to_create_docs if doc["_id"] not in created_ids]
    to_update_docs.extend(not_created_docs)
    updated_ids = ASTRADB.update_documents(to_update_docs)
    all_ids = set(created_ids) | set(updated_ids)

    to_cache = [doc for doc in all_docs if doc["_id"] in all_ids]
    VECTORCACHE.add_astra_doc(to_cache)
    return len(all_ids)


# ---- Worker and batch handlers ----
def init_worker(enable_vector=False):
    global LABEL_DB_READY
    global VECTORCACHE, ASTRADB
    global VECTOR_ITEM_FILTER, VECTOR_EMBEDDER

    if not LABEL_DB_READY:
        WikidataLabel.initialize_database()
        LABEL_DB_READY = True

    if enable_vector and any(x is None for x in (VECTOR_ITEM_FILTER, VECTOR_EMBEDDER, VECTORCACHE, ASTRADB)):
        VECTOR_ITEM_FILTER = WikidataItemFilter(lang=LANG, fallback_lang=FALLBACK_LANG)
        VECTOR_EMBEDDER = JinaAIAPIEmbedder(config_path=JINA_API_PATH)
        VECTORCACHE = WikidataVectorCache(lang=LANG, data_dir="./data/Wikidata/")
        ASTRADB = AstraDBConnect(lang=LANG, config_path=ASTRA_API_PATH)


# ---- Orchestration ----
def create_dump_reader():
    global FORCE_DOWNLOAD_DUMP

    check_wdtextifier_stack()
    reader = WikidataDumpReader(
        DUMP_PATH,
        num_processes=NUM_PROCESSES,
        queue_size=READER_QUEUE_SIZE,
        batch_size=READER_BATCH_SIZE,
    )

    if FORCE_DOWNLOAD_DUMP or (not os.path.exists(DUMP_PATH)):
        print(f"File {DUMP_PATH} does not exist. Downloading...")
        reader.download()
        FORCE_DOWNLOAD_DUMP = False

    return reader


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


def run_labels_stage():
    print("Running label pass")
    reset_runtime_state()
    reader = create_dump_reader()
    reader.run(
        save_labels,
        handler_receives_batch=True,
        init_consumer=init_worker,
        init_consumer_args=(False,),
    )


def run_wd_to_hf_stage():
    global HF_PUBLISHER

    print("Running full Wikidata -> HF pass")
    reset_runtime_state()
    reader = create_dump_reader()
    HF_PUBLISHER = WikidataHFDatasetPublisher(
        branch=HF_BRANCH,
        config_path=WD_HF_API_PATH,
        storage_chunk_size=HF_CHUNK_SIZE,
        memory_chunk_size=HF_BATCH_SIZE,
        queue_size=HF_QUEUE_SIZE,
        data_dir=f"data/{LANG}",
    )

    try:
        reader.run(
            push_to_hf,
            handler_receives_batch=True,
            init_consumer=init_worker,
            init_consumer_args=(False,),
        )
    finally:
        if HF_PUBLISHER is not None:
            HF_PUBLISHER.flush()


def run_vector_stages_per_language():
    global LANG, FALLBACK_LANG, VECTOR_HF_BRANCH, HF_PUBLISHER

    languages = WD_LANGS or (LANG,)
    default_fallback = os.environ.get("FALLBACK_LANG", FALLBACK_LANG)
    base_vector_hf_branch = VECTOR_HF_BRANCH
    multiple_languages = len(languages) > 1

    for lang in languages:
        fallback = os.environ.get(
            f"FALLBACK_LANG_{lang.upper()}",
            default_fallback or lang,
        )
        vector_hf_branch = (
            f"{base_vector_hf_branch}-{lang}" if multiple_languages else base_vector_hf_branch
        )

        print(f"Running vector stages for language={lang} (fallback={fallback})")
        LANG = lang
        FALLBACK_LANG = fallback
        VECTOR_HF_BRANCH = vector_hf_branch
        reset_runtime_state()

        if SAVE_TO_VECTORDB:
            reader = create_dump_reader()
            reader.run(
                push_to_vectorDB,
                handler_receives_batch=True,
                init_consumer=init_worker,
                init_consumer_args=(True,),
            )

        if SAVE_VECTORS_TO_HF:
            HF_PUBLISHER = WikidataHFDatasetPublisher(
                branch=VECTOR_HF_BRANCH,
                config_path=VECTORS_HF_API_PATH,
                storage_chunk_size=HF_CHUNK_SIZE,
                memory_chunk_size=HF_BATCH_SIZE,
                queue_size=HF_QUEUE_SIZE,
                data_dir=f"data/{LANG}",
            )
            try:
                save_vectors_to_hf()
            finally:
                HF_PUBLISHER.flush()


def run_pipeline():
    if SAVE_LABELS:
        run_labels_stage()

    if SAVE_WD_TO_HF:
        run_wd_to_hf_stage()

    if SAVE_TO_VECTORDB or SAVE_VECTORS_TO_HF:
        run_vector_stages_per_language()


if __name__ == "__main__":
    run_pipeline()
