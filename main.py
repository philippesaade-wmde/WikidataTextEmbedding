"""Run Wikidata dump processing and publishing stages."""

import os

from WikidataTextifier.src import JSONNormalizer, LazyLabelFactory, WikidataLabel

from src.JinaAI import JinaAIAPIEmbedder, JinaAITokenizer
from src.runStats import RunStatsTracker
from src.utils import (
    check_wdtextifier_stack,
    chunk_item_text,
    extract_instanceof,
    extract_pids,
)
from src.WikidataDumpReader import WikidataDumpReader
from src.WikidataFilter import (
    WikidataNoSitelinkFilter,
    WikidataPropertyFilter,
    WikidataScholarlyArticleFilter,
    WikidataSitelinkFilter,
)
from src.wikidataHuggingFace import WikidataHFDatasetPublisher
from src.WikidataJSONCleaner import WikidataJSONCleaner
from src.wikidataVectorCache import get_db_connection
from src.wikidataVectorDB import AstraDBConnect

# ---- Runtime config ----
READER_QUEUE_SIZE = int(os.environ.get("READER_QUEUE_SIZE", "128"))
READER_BATCH_SIZE = int(os.environ.get("READER_BATCH_SIZE", "16"))
NUM_PROCESSES = int(os.environ.get("NUM_PROCESSES", "4"))
DUMP_PATH = os.environ.get("DUMP_PATH", "data/wd_dump.gz")
LANG = os.environ.get("WD_LANG", os.environ.get("LANG", "en"))
FALLBACK_LANG = os.environ.get("FALLBACK_LANG", LANG)
WD_LANGS = tuple(
    lang.strip() for lang in os.environ.get("WD_LANGS", "").split(",") if lang.strip()
)

JINA_API_PATH = os.environ.get("JINA_API_PATH", "./API_tokens/jina_api.json")
ASTRA_API_PATH = os.environ.get("ASTRA_API_PATH", "./API_tokens/datastax_api.json")
WD_HF_API_PATH = os.environ.get("WD_HF_API_PATH", "./API_tokens/wd_hf_api.json")
VECTORS_HF_API_PATH = os.environ.get(
    "VECTORS_HF_API_PATH", "./API_tokens/vectors_hf_api.json"
)
HF_DATA_DIR = os.environ.get("HF_DATA_DIR", "data")
HF_CHUNK_SIZE = int(os.environ.get("HF_CHUNK_SIZE", "1000"))
HF_BATCH_SIZE = int(os.environ.get("HF_BATCH_SIZE", "32"))
HF_QUEUE_SIZE = int(os.environ.get("HF_QUEUE_SIZE", "128"))
DUMP_DATE = os.environ.get("DUMP_DATE")
HF_BRANCH = os.environ.get("HF_BRANCH")
VECTOR_HF_BRANCH = os.environ.get("VECTOR_HF_BRANCH")
MERGE_HF_TO_MAIN = os.environ.get("MERGE_HF_TO_MAIN", "false").lower() == "true"
PROPERTY_CONSTRAINT_PIDS = tuple(
    pid.strip()
    for pid in os.environ.get("PROPERTY_CONSTRAINT_PIDS", "P2302").split(",")
    if pid.strip()
)

SAVE_WD_TO_HF = os.environ.get("SAVE_WD_TO_HF", "false").lower() == "true"
SAVE_VECTORS_TO_HF = os.environ.get("SAVE_VECTORS_TO_HF", "false").lower() == "true"
SAVE_TO_VECTORDB = os.environ.get("SAVE_TO_VECTORDB", "false").lower() == "true"
SAVE_SITELINK_VECTORS = (
    os.environ.get("SAVE_SITELINK_VECTORS", "true").lower() == "true"
)
SAVE_NOSITELINK_VECTORS = (
    os.environ.get("SAVE_NOSITELINK_VECTORS", "true").lower() == "true"
)
SAVE_LABELS = os.environ.get("SAVE_LABELS", "false").lower() == "true"
DELETE_STALE_VECTORS = os.environ.get("DELETE_STALE_VECTORS", "false").lower() == "true"
FORCE_DOWNLOAD_DUMP = os.environ.get("FORCE_DOWNLOAD_DUMP", "false").lower() == "true"
RUN_STATS_PATH = os.environ.get("RUN_STATS_PATH", "data/run_stats.json")

VECTOR_TARGETS = []
if SAVE_SITELINK_VECTORS:
    VECTOR_TARGETS.extend(
        (
            {
                "entity_type": "items",
                "counter_prefix": "vector",
            },
            {
                "entity_type": "properties",
                "counter_prefix": "vector",
            },
        )
    )
if SAVE_NOSITELINK_VECTORS:
    VECTOR_TARGETS.append(
        {
            "entity_type": "items_nositelinks",
            "counter_prefix": "vector_nositelinks",
        }
    )
VECTOR_TARGETS = tuple(VECTOR_TARGETS)
VECTOR_ENTITY_TYPES = tuple(target["entity_type"] for target in VECTOR_TARGETS)


# ---- Process-local runtime state ----
TEXT_PROPERTY_FILTER = None
TEXT_TOKENIZER = None
VECTOR_ITEM_FILTER = None
VECTOR_NOSITELINK_FILTER = None
VECTOR_EMBEDDER = None
VECTOR_CACHES = None
ASTRADBS = None
HF_PUBLISHER = None
WD_HF_SCHOLARLY_FILTER = None
LABEL_DB_READY = False
dump_reader = None
STATS_TRACKER = None


# ---- Transformation steps ----
def save_labels(items):
    """Persist Wikidata labels for a batch of dump items."""
    data = {item["id"]: {"labels": item["labels"]} for item in items}
    if not data:
        return
    compressed = WikidataLabel._compress_labels(data)
    WikidataLabel.add_bulk_labels(
        [{"id": qid, "labels": labels} for qid, labels in compressed.items()]
    )
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add("labels_saved", len(data))


def item_to_json(item, label_factory=None):
    """Convert a dump item into cleaned JSON with resolved labels."""
    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    clean_json = WikidataJSONCleaner.clean_entity(item, label_factory.create)
    label_factory.resolve_all()
    return label_factory.resolve_labels_in_json(clean_json)


def item_to_text(item, label_factory=None):
    """Convert a dump item into chunked text documents for vector storage."""
    global TEXT_PROPERTY_FILTER, TEXT_TOKENIZER

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    last_modified = item.get("modified")
    property_datatype = item.get("datatype") if item["id"].startswith("P") else None

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
    item = TEXT_PROPERTY_FILTER.sort_and_filter_textifier(
        item, drop_claim_pids=drop_claim_pids
    )

    label_factory.resolve_all()

    if TEXT_TOKENIZER is None:
        TEXT_TOKENIZER = JinaAITokenizer()
    chunks = chunk_item_text(item, TEXT_TOKENIZER, max_length=1024, lang=LANG)

    metadata = {
        "Label": item.label,
        "Description": item.description,
        "QID" if item.id.startswith("Q") else "PID": item.id,
        "Language": LANG,
        "InstanceOf": extract_instanceof(item),
        "Properties": extract_pids(item),
        "LastModified": last_modified,
        "DumpDate": DUMP_DATE,
    }
    if item.id.startswith("P"):
        metadata["DataType"] = property_datatype

    return [
        {
            "_id": f"{item.id}_{LANG}_{i + 1}",
            "content": chunk,
            "metadata": metadata,
        }
        for i, chunk in enumerate(chunks)
    ]


# ---- Sink steps ----
def push_to_hf(items, label_factory=None):
    """Publish filtered dump items to the configured Hugging Face dataset."""
    global WD_HF_SCHOLARLY_FILTER

    if HF_PUBLISHER is None:
        raise RuntimeError("HF publisher is not initialized in this process.")

    existing_ids = HF_PUBLISHER.existing_ids([item["id"] for item in items if item])
    if existing_ids:
        items = [item for item in items if item and item["id"] not in existing_ids]
        if STATS_TRACKER is not None:
            STATS_TRACKER.counter_add("wd_hf_skipped_existing", len(existing_ids))
    if not items:
        return 0

    if WD_HF_SCHOLARLY_FILTER is None:
        WD_HF_SCHOLARLY_FILTER = WikidataScholarlyArticleFilter(
            lang=LANG,
            fallback_lang=FALLBACK_LANG,
        )
    before_filter = len(items)
    items = [
        item for item in items if WD_HF_SCHOLARLY_FILTER.not_scholarly_article(item)
    ]
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add("wd_hf_skipped_scholarly", before_filter - len(items))
    if not items:
        return 0

    if label_factory is None:
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    rows = [item_to_json(item, label_factory=label_factory) for item in items]
    pushed = HF_PUBLISHER.publish_wd_batch(rows)
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add("wd_hf_rows", pushed)
    return pushed


def save_vectors_to_hf():
    """Publish cached vector rows from SQLite to Hugging Face."""
    if HF_PUBLISHER is None:
        raise RuntimeError("HF publisher is not initialized in this process.")

    total = 0
    for entity_type in VECTOR_ENTITY_TYPES:
        vector_cache = get_db_connection(
            lang=LANG,
            entity_type=entity_type,
            data_dir="./data/Wikidata/",
        )
        for vectors in vector_cache.iter_batches(batch_size=HF_CHUNK_SIZE):
            existing_ids = HF_PUBLISHER.existing_ids(
                [vector.get("id") for vector in vectors if vector and vector.get("id")]
            )
            if existing_ids:
                vectors = [
                    vector
                    for vector in vectors
                    if vector and vector.get("id") not in existing_ids
                ]
            total += HF_PUBLISHER.publish_vector_batch(vectors)
    return total


def push_vector_batch(
    items, vector_cache, astra_db, counter_prefix, label_factory=None
):
    """Embed and persist one filtered item batch to AstraDB and the local cache."""
    to_update, to_create = vector_cache.filter_for_update(items)
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add(f"{counter_prefix}_update_items", len(to_update))
        STATS_TRACKER.counter_add(f"{counter_prefix}_create_items", len(to_create))

    if DUMP_DATE:
        changed_ids = {item["id"] for item in to_update + to_create}
        unchanged_ids = [item["id"] for item in items if item["id"] not in changed_ids]
        if unchanged_ids:
            vector_cache.touch_last_dump(unchanged_ids, DUMP_DATE)

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

    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add(f"{counter_prefix}_candidate_docs", len(all_docs))

    vectors = VECTOR_EMBEDDER.embed_documents([doc["content"] for doc in all_docs])
    for doc, vector in zip(all_docs, vectors):
        doc["$vector"] = vector

    created_ids = astra_db.create_documents(to_create_docs)
    not_created_docs = [doc for doc in to_create_docs if doc["_id"] not in created_ids]
    to_update_docs.extend(not_created_docs)
    updated_ids = astra_db.update_documents(to_update_docs)
    all_ids = set(created_ids) | set(updated_ids)
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add(f"{counter_prefix}_created_docs", len(created_ids))
        STATS_TRACKER.counter_add(f"{counter_prefix}_updated_docs", len(updated_ids))
        STATS_TRACKER.counter_add(f"{counter_prefix}_saved_docs", len(all_ids))

    to_cache = [doc for doc in all_docs if doc["_id"] in all_ids]
    vector_cache.add_astra_doc(to_cache, dump_date=DUMP_DATE)
    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add(f"{counter_prefix}_cached_docs", len(to_cache))
    return len(all_ids)


def push_to_vectorDB(items, label_factory=None):
    """Filter a dump batch and push matching entities to vector storage."""
    if any(
        x is None
        for x in (
            VECTOR_ITEM_FILTER,
            VECTOR_NOSITELINK_FILTER,
            VECTOR_EMBEDDER,
            VECTOR_CACHES,
            ASTRADBS,
        )
    ):
        init_worker(enable_vector=True)

    if STATS_TRACKER is not None:
        STATS_TRACKER.counter_add("vector_input_items", len(items))

    batches = []
    for target in VECTOR_TARGETS:
        entity_type = target["entity_type"]
        if entity_type == "items":
            target_items = [
                item
                for item in items
                if item.get("id", "").startswith("Q")
                and VECTOR_ITEM_FILTER.filter(item)
            ]
        elif entity_type == "properties":
            target_items = [
                item
                for item in items
                if item.get("id", "").startswith("P")
                and VECTOR_ITEM_FILTER.filter(item)
            ]
        elif entity_type == "items_nositelinks":
            target_items = [
                item for item in items if VECTOR_NOSITELINK_FILTER.filter(item)
            ]
        else:
            raise ValueError(f"Unknown vector entity type: {entity_type}")
        batches.append((target, target_items))

    if STATS_TRACKER is not None:
        for target, target_items in batches:
            STATS_TRACKER.counter_add(
                f"{target['counter_prefix']}_filtered_items",
                len(target_items),
            )

    if label_factory is None and any(target_items for _, target_items in batches):
        label_factory = LazyLabelFactory(lang=LANG, fallback_lang=FALLBACK_LANG)

    saved = 0
    for target, target_items in batches:
        entity_type = target["entity_type"]
        saved += push_vector_batch(
            target_items,
            VECTOR_CACHES[entity_type],
            ASTRADBS[entity_type],
            target["counter_prefix"],
            label_factory=label_factory,
        )
    return saved


# ---- Worker and batch handlers ----
def init_worker(enable_vector=False):
    """Initialize process-local label and vector resources."""
    global LABEL_DB_READY
    global VECTOR_CACHES, ASTRADBS
    global VECTOR_ITEM_FILTER, VECTOR_NOSITELINK_FILTER, VECTOR_EMBEDDER

    if not LABEL_DB_READY:
        WikidataLabel.initialize_database()
        LABEL_DB_READY = True

    if enable_vector and any(
        x is None
        for x in (
            VECTOR_ITEM_FILTER,
            VECTOR_NOSITELINK_FILTER,
            VECTOR_EMBEDDER,
            VECTOR_CACHES,
            ASTRADBS,
        )
    ):
        VECTOR_ITEM_FILTER = WikidataSitelinkFilter(
            lang=LANG, fallback_lang=FALLBACK_LANG
        )
        VECTOR_NOSITELINK_FILTER = WikidataNoSitelinkFilter(
            lang=LANG, fallback_lang=FALLBACK_LANG
        )
        VECTOR_EMBEDDER = JinaAIAPIEmbedder(config_path=JINA_API_PATH)
        VECTOR_CACHES = {
            entity_type: get_db_connection(
                lang=LANG,
                entity_type=entity_type,
                data_dir="./data/Wikidata/",
            )
            for entity_type in VECTOR_ENTITY_TYPES
        }
        ASTRADBS = {
            entity_type: AstraDBConnect(
                lang=LANG,
                entity_type=entity_type,
                config_path=ASTRA_API_PATH,
            )
            for entity_type in VECTOR_ENTITY_TYPES
        }


# ---- Orchestration ----
def create_dump_reader():
    """Create the dump reader and resolve dump-derived branch names."""
    global FORCE_DOWNLOAD_DUMP, DUMP_DATE, HF_BRANCH, VECTOR_HF_BRANCH

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

    if not DUMP_DATE or not HF_BRANCH or not VECTOR_HF_BRANCH:
        dump_date = reader.get_dump_date()
        if not DUMP_DATE:
            DUMP_DATE = dump_date
        if not HF_BRANCH:
            HF_BRANCH = dump_date.replace("-", "")
        if not VECTOR_HF_BRANCH:
            VECTOR_HF_BRANCH = HF_BRANCH
    print(
        f"Dump date: {DUMP_DATE}\n HF branch: {HF_BRANCH}\n Vector HF branch: {VECTOR_HF_BRANCH}"
    )

    return reader


def resolve_hf_branches_without_dump():
    """Resolve Hugging Face branch names without opening the dump reader."""
    global DUMP_DATE, HF_BRANCH, VECTOR_HF_BRANCH

    if not DUMP_DATE and (not HF_BRANCH or not VECTOR_HF_BRANCH):
        date_file = DUMP_PATH + ".date"
        if os.path.exists(date_file):
            with open(date_file) as f:
                DUMP_DATE = f.read().strip()
        elif os.path.exists(DUMP_PATH):
            DUMP_DATE = WikidataDumpReader(DUMP_PATH).get_dump_date()
    if not HF_BRANCH and DUMP_DATE:
        HF_BRANCH = DUMP_DATE.replace("-", "")
    if not VECTOR_HF_BRANCH:
        VECTOR_HF_BRANCH = HF_BRANCH

    if SAVE_WD_TO_HF and MERGE_HF_TO_MAIN and not HF_BRANCH:
        raise ValueError(
            "Set HF_BRANCH or DUMP_DATE when MERGE_HF_TO_MAIN=true and SAVE_WD_TO_HF=true."
        )
    if SAVE_VECTORS_TO_HF and MERGE_HF_TO_MAIN and not VECTOR_HF_BRANCH:
        raise ValueError(
            "Set VECTOR_HF_BRANCH, HF_BRANCH, or DUMP_DATE when MERGE_HF_TO_MAIN=true "
            "and SAVE_VECTORS_TO_HF=true."
        )

    print(
        f"Dump date: {DUMP_DATE}\n HF branch: {HF_BRANCH}\n Vector HF branch: {VECTOR_HF_BRANCH}"
    )


def reset_runtime_state():
    """Clear process-local caches and counters between pipeline stages."""
    global dump_reader, HF_PUBLISHER
    global WD_HF_SCHOLARLY_FILTER
    global TEXT_PROPERTY_FILTER, TEXT_TOKENIZER
    global VECTOR_ITEM_FILTER, VECTOR_NOSITELINK_FILTER, VECTOR_EMBEDDER
    global VECTOR_CACHES, ASTRADBS

    dump_reader = None
    HF_PUBLISHER = None
    WD_HF_SCHOLARLY_FILTER = None
    TEXT_PROPERTY_FILTER = None
    TEXT_TOKENIZER = None
    VECTOR_ITEM_FILTER = None
    VECTOR_NOSITELINK_FILTER = None
    VECTOR_EMBEDDER = None
    VECTOR_CACHES = None
    ASTRADBS = None
    if STATS_TRACKER is not None:
        STATS_TRACKER.clear_counters()


def run_labels_stage():
    """Run the label extraction stage."""
    stage_name = "labels"
    print("Running label pass")
    reset_runtime_state()
    reader = create_dump_reader()
    counters = STATS_TRACKER.start_counters(("labels_saved",))
    try:
        reader.run(
            save_labels,
            handler_receives_batch=True,
            init_consumer=init_worker,
            init_consumer_args=(False,),
        )
    except Exception as exc:
        STATS_TRACKER.record_error(stage_name, exc=exc)
        raise
    finally:
        STATS_TRACKER.clear_counters()

    stage_stats = STATS_TRACKER.read_counters(counters)
    stage_stats.update(
        {
            "entities_processed": int(reader.iterations.value),
            "handler_errors": int(reader.handler_errors.value),
        }
    )
    STATS_TRACKER.set_stage_stats("labels", stage_stats)
    STATS_TRACKER.record_error(stage_name, stage_stats["handler_errors"])


def run_wd_to_hf_stage():
    """Run the Wikidata dump to Hugging Face dataset stage."""
    global HF_PUBLISHER

    stage_name = "wd_to_hf"
    reset_runtime_state()
    if MERGE_HF_TO_MAIN:
        print(f"Merging Wikidata HF branch {HF_BRANCH} -> main")
        stage_stats = {
            "branch": HF_BRANCH,
            "merged_to_main": True,
            "merge_to_main": WikidataHFDatasetPublisher.merge_to_main(
                branch=HF_BRANCH,
                config_path=WD_HF_API_PATH,
                batch_size=HF_CHUNK_SIZE,
            ),
        }
        STATS_TRACKER.set_stage_stats("wd_to_hf", stage_stats)
        STATS_TRACKER.record_error(stage_name, 0)
        return

    print("Running full Wikidata -> HF pass")
    reader = create_dump_reader()
    counters = STATS_TRACKER.start_counters(
        (
            "wd_hf_rows",
            "wd_hf_skipped_existing",
            "wd_hf_skipped_scholarly",
        )
    )
    HF_PUBLISHER = WikidataHFDatasetPublisher(
        branch=HF_BRANCH,
        config_path=WD_HF_API_PATH,
        storage_chunk_size=HF_CHUNK_SIZE,
        memory_chunk_size=HF_BATCH_SIZE,
        queue_size=HF_QUEUE_SIZE,
        data_dir=HF_DATA_DIR,
    )

    try:
        reader.run(
            push_to_hf,
            handler_receives_batch=True,
            init_consumer=init_worker,
            init_consumer_args=(False,),
        )
    except Exception as exc:
        STATS_TRACKER.record_error(stage_name, exc=exc)
        raise
    finally:
        STATS_TRACKER.clear_counters()
        if HF_PUBLISHER is not None:
            HF_PUBLISHER.flush()

    stage_stats = STATS_TRACKER.read_counters(counters)
    stage_stats.update(
        {
            "branch": HF_BRANCH,
            "data_dir": HF_DATA_DIR,
            "entities_processed": int(reader.iterations.value),
            "handler_errors": int(reader.handler_errors.value),
        }
    )
    STATS_TRACKER.set_stage_stats("wd_to_hf", stage_stats)
    STATS_TRACKER.record_error(stage_name, stage_stats["handler_errors"])


def run_vectordb_stages():
    """Run AstraDB vector upsert and optional stale-deletion stages."""
    global LANG, FALLBACK_LANG

    languages = WD_LANGS or (LANG,)
    default_fallback = os.environ.get("FALLBACK_LANG", FALLBACK_LANG)

    for lang in languages:
        fallback = os.environ.get(
            f"FALLBACK_LANG_{lang.upper()}",
            default_fallback or lang,
        )

        print(f"Running vector stages for language={lang} (fallback={fallback})")
        LANG = lang
        FALLBACK_LANG = fallback
        reset_runtime_state()
        lang_stats = STATS_TRACKER.get_language_stats(
            lang,
            {
                "language": lang,
                "fallback_lang": fallback,
                "vector_hf_branch": VECTOR_HF_BRANCH,
            },
        )

        stage_name = f"vectordb:{lang}"
        reader = create_dump_reader()
        counters = STATS_TRACKER.start_counters(
            (
                "vector_input_items",
                "vector_filtered_items",
                "vector_update_items",
                "vector_create_items",
                "vector_candidate_docs",
                "vector_created_docs",
                "vector_updated_docs",
                "vector_saved_docs",
                "vector_cached_docs",
                "vector_nositelinks_filtered_items",
                "vector_nositelinks_update_items",
                "vector_nositelinks_create_items",
                "vector_nositelinks_candidate_docs",
                "vector_nositelinks_created_docs",
                "vector_nositelinks_updated_docs",
                "vector_nositelinks_saved_docs",
                "vector_nositelinks_cached_docs",
            )
        )
        stage_exc = None
        try:
            reader.run(
                push_to_vectorDB,
                handler_receives_batch=True,
                init_consumer=init_worker,
                init_consumer_args=(True,),
            )
        except Exception as exc:  # noqa: BLE001
            stage_exc = exc
            STATS_TRACKER.record_error(stage_name, exc=exc)
        finally:
            vectordb_stats = STATS_TRACKER.read_counters(counters)
            vectordb_stats.update(
                {
                    "entities_processed": int(reader.iterations.value),
                    "handler_errors": int(reader.handler_errors.value),
                }
            )
            lang_stats["vectordb"] = vectordb_stats
            STATS_TRACKER.record_error(stage_name, vectordb_stats["handler_errors"])
            STATS_TRACKER.clear_counters()

        if stage_exc is not None:
            raise stage_exc

        if DELETE_STALE_VECTORS:
            stale_targets = []
            for target in VECTOR_TARGETS:
                entity_type = target["entity_type"]
                cache = get_db_connection(
                    lang=LANG,
                    entity_type=entity_type,
                    data_dir="./data/Wikidata/",
                )
                stale_targets.append((target, cache, cache.count_stale(DUMP_DATE)))

            stale_count_by_entity_type = {
                target["entity_type"]: stale_count
                for target, _, stale_count in stale_targets
            }
            total_stale_count = sum(stale_count_by_entity_type.values())
            print(
                f"\nStale cache entries for '{lang}' (last_dump < {DUMP_DATE}): {total_stale_count}"
            )
            for entity_type, stale_count in stale_count_by_entity_type.items():
                print(f"  {entity_type}: {stale_count}")
            try:
                confirmed = (
                    input("Delete these entries? [y/N]: ").strip().lower() == "y"
                )
            except EOFError:
                confirmed = False
            astra_deleted_by_entity_type = {}
            if confirmed:
                for target, cache, _ in stale_targets:
                    entity_type = target["entity_type"]
                    astra = AstraDBConnect(
                        lang=LANG,
                        entity_type=entity_type,
                        config_path=ASTRA_API_PATH,
                    )
                    astra_deleted_by_entity_type[entity_type] = 0
                    for batch_ids in cache.iter_stale_batches(DUMP_DATE):
                        astra_deleted_by_entity_type[entity_type] += (
                            astra.delete_documents(batch_ids)
                        )
                print(
                    f"Deleted {sum(astra_deleted_by_entity_type.values())} documents from AstraDB "
                    f"and {total_stale_count} entries from local cache."
                )
            else:
                print("Deletion skipped.")
            lang_stats["stale_deletion"] = {
                "stale_count": total_stale_count,
                "stale_count_by_entity_type": stale_count_by_entity_type,
                "confirmed": confirmed,
                "astra_deleted": sum(astra_deleted_by_entity_type.values()),
                "astra_deleted_by_entity_type": astra_deleted_by_entity_type,
            }


def run_vectors_to_hf_stage():
    """Publish locally cached vectors to a Hugging Face dataset branch."""
    global LANG, FALLBACK_LANG, HF_PUBLISHER

    languages = WD_LANGS or (LANG,)
    default_fallback = os.environ.get("FALLBACK_LANG", FALLBACK_LANG)

    if MERGE_HF_TO_MAIN:
        reset_runtime_state()
        stage_name = "vectors_to_hf"
        print(f"Merging vector HF branch {VECTOR_HF_BRANCH} -> main")
        merge_stats = WikidataHFDatasetPublisher.merge_to_main(
            branch=VECTOR_HF_BRANCH,
            config_path=VECTORS_HF_API_PATH,
            batch_size=HF_CHUNK_SIZE,
        )
        for lang in languages:
            fallback = os.environ.get(
                f"FALLBACK_LANG_{lang.upper()}",
                default_fallback or lang,
            )
            lang_stats = STATS_TRACKER.get_language_stats(
                lang,
                {
                    "language": lang,
                    "fallback_lang": fallback,
                    "vector_hf_branch": VECTOR_HF_BRANCH,
                },
            )
            lang_stats["vectors_to_hf"] = {
                "branch": VECTOR_HF_BRANCH,
                "merged_to_main": True,
                "merge_to_main": merge_stats,
            }
        STATS_TRACKER.record_error(stage_name, 0)
        return

    for lang in languages:
        fallback = os.environ.get(
            f"FALLBACK_LANG_{lang.upper()}",
            default_fallback or lang,
        )

        print(f"Running vector stages for language={lang} (fallback={fallback})")
        LANG = lang
        FALLBACK_LANG = fallback
        reset_runtime_state()
        lang_stats = STATS_TRACKER.get_language_stats(
            lang,
            {
                "language": lang,
                "fallback_lang": fallback,
                "vector_hf_branch": VECTOR_HF_BRANCH,
            },
        )

        stage_name = f"vectors_to_hf:{lang}"
        HF_PUBLISHER = WikidataHFDatasetPublisher(
            branch=VECTOR_HF_BRANCH,
            config_path=VECTORS_HF_API_PATH,
            storage_chunk_size=HF_CHUNK_SIZE,
            memory_chunk_size=HF_BATCH_SIZE,
            queue_size=HF_QUEUE_SIZE,
            data_dir=f"{HF_DATA_DIR}/{LANG}",
        )
        vectors_pushed = 0
        try:
            vectors_pushed = save_vectors_to_hf()
        except Exception as exc:
            STATS_TRACKER.record_error(stage_name, exc=exc)
            raise
        finally:
            HF_PUBLISHER.flush()
        vectors_to_hf_stats = {
            "branch": VECTOR_HF_BRANCH,
            "data_dir": f"{HF_DATA_DIR}/{LANG}",
            "entity_types": list(VECTOR_ENTITY_TYPES),
            "rows_pushed": int(vectors_pushed),
        }
        lang_stats["vectors_to_hf"] = vectors_to_hf_stats


def run_pipeline():
    """Run all enabled pipeline stages."""
    global STATS_TRACKER

    if (
        SAVE_TO_VECTORDB
        or DELETE_STALE_VECTORS
        or (SAVE_VECTORS_TO_HF and not MERGE_HF_TO_MAIN)
    ) and not VECTOR_TARGETS:
        raise ValueError(
            "At least one of SAVE_SITELINK_VECTORS or SAVE_NOSITELINK_VECTORS must be true "
            "when running vector stages."
        )

    if (
        SAVE_LABELS
        or SAVE_TO_VECTORDB
        or DELETE_STALE_VECTORS
        or ((SAVE_WD_TO_HF or SAVE_VECTORS_TO_HF) and not MERGE_HF_TO_MAIN)
    ):
        create_dump_reader()
    else:
        resolve_hf_branches_without_dump()

    stats_config = {
        "dump_path": DUMP_PATH,
        "num_processes": NUM_PROCESSES,
        "reader_queue_size": READER_QUEUE_SIZE,
        "reader_batch_size": READER_BATCH_SIZE,
        "hf_chunk_size": HF_CHUNK_SIZE,
        "hf_batch_size": HF_BATCH_SIZE,
        "hf_queue_size": HF_QUEUE_SIZE,
        "wd_lang": LANG,
        "wd_langs": list(WD_LANGS),
        "fallback_lang": FALLBACK_LANG,
        "save_labels": SAVE_LABELS,
        "save_wd_to_hf": SAVE_WD_TO_HF,
        "save_to_vectordb": SAVE_TO_VECTORDB,
        "save_vectors_to_hf": SAVE_VECTORS_TO_HF,
        "save_sitelink_vectors": SAVE_SITELINK_VECTORS,
        "save_nositelink_vectors": SAVE_NOSITELINK_VECTORS,
        "vector_entity_types": list(VECTOR_ENTITY_TYPES),
        "merge_hf_to_main": MERGE_HF_TO_MAIN,
        "delete_stale_vectors": DELETE_STALE_VECTORS,
        "hf_branch": HF_BRANCH,
        "vector_hf_branch": VECTOR_HF_BRANCH,
    }
    STATS_TRACKER = RunStatsTracker(RUN_STATS_PATH, stats_config)

    try:
        if SAVE_LABELS:
            run_labels_stage()

        if SAVE_WD_TO_HF:
            run_wd_to_hf_stage()

        if SAVE_TO_VECTORDB or DELETE_STALE_VECTORS:
            run_vectordb_stages()

        if SAVE_VECTORS_TO_HF:
            run_vectors_to_hf_stage()

        STATS_TRACKER.finalize("completed")

    except Exception:
        STATS_TRACKER.finalize("failed")
        raise


if __name__ == "__main__":
    run_pipeline()
