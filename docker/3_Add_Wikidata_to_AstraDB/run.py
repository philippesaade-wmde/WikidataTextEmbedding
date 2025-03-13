import json
import os
import pickle
import hashlib

from datetime import datetime
from tqdm import tqdm

from src.wikidataLangDB import Session, WikidataLang
from src.wikidataEmbed import WikidataTextifier
from src.wikidataRetriever import AstraDBConnect, KeywordSearchConnect

MODEL = os.getenv("MODEL", "jina")
SAMPLE = os.getenv("SAMPLE", "false").lower() == "true"
SAMPLE_PATH = os.getenv(
    "SAMPLE_PATH", "../data/Evaluation Data/Sample IDs (EN).pkl"
)
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))
QUERY_BATCH_SIZE = int(os.getenv("QUERY_BATCH_SIZE", 1000))
OFFSET = int(os.getenv("OFFSET", 0))
API_KEY_FILENAME = os.getenv("API_KEY", None)
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
LANGUAGE = os.getenv("LANGUAGE", 'en')
TEXTIFIER_LANGUAGE = os.getenv("TEXTIFIER_LANGUAGE", None)
DUMPDATE = os.getenv("DUMPDATE", '09/18/2024')

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH = os.getenv("ELASTICSEARCH", "false").lower() == "true"

# TODO: refactor script into function and call after __name__ == "__main__"
# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not TEXTIFIER_LANGUAGE:
    TEXTIFIER_LANGUAGE = LANGUAGE

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]

datastax_token = json.load(open(f"../API_tokens/{API_KEY_FILENAME}"))

textifier = WikidataTextifier(
    language=LANGUAGE,
    langvar_filename=TEXTIFIER_LANGUAGE
)

if ELASTICSEARCH:
    graph_store = KeywordSearchConnect(
        ELASTICSEARCH_URL,
        index_name=COLLECTION_NAME
    )
else:
    graph_store = AstraDBConnect(
        datastax_token,
        COLLECTION_NAME,
        model=MODEL,
        batch_size=EMBED_BATCH_SIZE,
        cache_embeddings=False
    )

# TODO: refactor script into function and call after __name__ == "__main__"
# Load the Sample IDs
sample_ids = None
if SAMPLE:
    sample_ids = pickle.load(open(SAMPLE_PATH, "rb"))
    sample_ids = sample_ids[sample_ids['In Wikipedia']]
    total_entities = len(sample_ids)

    def get_entity(session):
        sample_qids = list(sample_ids['QID'].values)[OFFSET:]
        sample_qid_batches = [
            sample_qids[i:i + QUERY_BATCH_SIZE]
            for i in range(0, len(sample_qids), QUERY_BATCH_SIZE)
        ]

        # For each batch of sample QIDs, fetch the entities from the database
        for qid_batch in sample_qid_batches:
            entities = session.query(WikidataLang).filter(
                    WikidataLang.id.in_(qid_batch)
                ).yield_per(QUERY_BATCH_SIZE)

            for entity in entities:
                yield entity
else:
    total_entities = 9203786

    def get_entity(session):
        entities = session.query(
            WikidataLang).offset(
                OFFSET
            ).yield_per(QUERY_BATCH_SIZE)

        for entity in entities:
            yield entity


if __name__ == "__main__":
    # TODO: refactor script into function and call after __name__ == "__main__"
    with tqdm(total=total_entities-OFFSET) as progressbar:
        with Session() as session:
            entity_generator = get_entity(session)
            doc_batch = []
            ids_batch = []

            for entity in entity_generator:
                progressbar.update(1)
                if ELASTICSEARCH:
                    chunks = [textifier.entity_to_text(entity)]
                else:
                    chunks = textifier.chunk_text(
                        entity,
                        graph_store.tokenizer,
                        max_length=graph_store.max_token_size
                    )

                for chunk_i in range(len(chunks)):
                    md5_hash = hashlib.md5(
                        chunks[chunk_i].encode('utf-8')
                    ).hexdigest()

                    metadata = {
                        "MD5": md5_hash,
                        "Label": entity.label,
                        "Description": entity.description,
                        "Aliases": entity.aliases,
                        "Date": datetime.now().isoformat(),
                        "QID": entity.id,
                        "ChunkID": chunk_i+1,
                        "Language": LANGUAGE,
                        "IsItem": ('Q' in entity.id),
                        "IsProperty": ('P' in entity.id),
                        "DumpDate": DUMPDATE
                    }
                    graph_store.add_document(
                        id=f"{entity.id}_{LANGUAGE}_{chunk_i+1}",
                        text=chunks[chunk_i],
                        metadata=metadata
                    )

                # tqdm is not working in docker compose.
                # This is the alternative
                tqdm.write(
                    progressbar.format_meter(
                        progressbar.n,
                        progressbar.total,
                        progressbar.format_dict["elapsed"]
                    )
                )

            graph_store.push_batch()
