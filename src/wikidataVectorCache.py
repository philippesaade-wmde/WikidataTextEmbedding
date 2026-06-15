"""
SQLite cache for Wikidata vector embeddings pushed to AstraDB.
"""
from sqlalchemy import Column, Text, DateTime, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import base64
import numpy as np
from datetime import datetime
from src.utils import normalize_datetime


class VectorType(TypeDecorator):
    """Custom SQLAlchemy type for Vector storage in SQLite."""
    impl = Text
    cache_ok = False

    def process_bind_param(self, value, dialect):
        arr = np.array(value, dtype='<f4')
        binary_data = arr.tobytes()
        return base64.b64encode(binary_data).decode('utf8')

    def process_result_value(self, vector, dialect):
        binary_data = base64.b64decode(vector)
        embedding_array = np.frombuffer(binary_data, dtype='<f4')
        return embedding_array.tolist()


def get_db_connection(lang="en", entity_type="items", data_dir="../data/Wikidata"):
    db_name = f"sqlite_wikidata_vectors_{entity_type}_{lang}.db"
    engine = create_engine(f'sqlite:///{data_dir}/{db_name}',
        pool_size=5,  # Limit the number of open connections
        max_overflow=10,  # Allow extra connections beyond pool_size
        pool_recycle=10  # Recycle connections every 10 seconds
    )

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class VectorCache(Base):
        __tablename__ = 'vectors'
        id = Column(Text, primary_key=True)
        vector = Column(VectorType)
        lang = Column(Text)
        wdid = Column(Text)
        last_updated = Column(DateTime, default=datetime.utcnow)
        last_dump = Column(DateTime, nullable=True)

        @staticmethod
        def add(data):
            """
            Bulk insert. Existing ones are updated.
            """
            with Session() as session:
                try:
                    for row in data:
                        row["last_updated"] = normalize_datetime(row.get("last_updated"))

                    session.execute(
                        text("INSERT INTO vectors (id, vector, lang, wdid, last_updated, last_dump) VALUES (:id, :vector, :lang, :wdid, :last_updated, :last_dump) ON CONFLICT(id) DO UPDATE SET vector = EXCLUDED.vector, lang = EXCLUDED.lang, wdid = EXCLUDED.wdid, last_updated = EXCLUDED.last_updated, last_dump = EXCLUDED.last_dump"),
                        data
                    )
                    session.commit()
                    return True
                except Exception as e:
                    session.rollback()
                    print(f"Error: {e}")
                    raise e

        @staticmethod
        def touch_last_dump(wdids, dump_date):
            """Set last_dump = dump_date for all rows whose wdid is in wdids."""
            if not wdids:
                return
            with Session() as session:
                session.query(VectorCache).filter(VectorCache.wdid.in_(wdids)).update(
                    {"last_dump": dump_date}, synchronize_session=False
                )
                session.commit()

        @staticmethod
        def count_stale(dump_date):
            """Return the number of rows with last_dump older than dump_date or NULL."""
            with Session() as session:
                return session.execute(
                    text("SELECT COUNT(*) FROM vectors WHERE last_dump IS NULL OR last_dump < :d"),
                    {"d": dump_date},
                ).scalar()

        @staticmethod
        def iter_stale_batches(dump_date, batch_size=1000):
            """Yield batches of stale row IDs, deleting each batch from SQLite after the caller resumes."""
            cursor = ""
            while True:
                with Session() as session:
                    batch = session.execute(
                        text("SELECT id FROM vectors WHERE (last_dump IS NULL OR last_dump < :d) AND id > :c ORDER BY id LIMIT :n"),
                        {"d": dump_date, "c": cursor, "n": batch_size},
                    ).scalars().all()
                batch = list(batch)
                if not batch:
                    break
                yield batch
                with Session() as session:
                    session.query(VectorCache).filter(VectorCache.id.in_(batch)).delete(synchronize_session=False)
                    session.commit()
                cursor = batch[-1]

        @staticmethod
        def filter_for_update(data):
            """Filter out IDs that are already in the database and haven't been updated since the last push."""
            with Session() as session:
                try:
                    ids = [f"{d['id']}_{lang}_1" for d in data]
                    existing_rows = (
                        session.query(VectorCache.wdid, VectorCache.last_updated)
                        .filter(VectorCache.id.in_(ids))
                        .all()
                    )
                    existing_dict = {
                        wdid: last_updated
                        for wdid, last_updated in existing_rows
                        if wdid
                    }

                    to_update = []
                    to_create = []
                    for d in data:
                        existing_last_updated = existing_dict.get(d['id'])
                        if not existing_last_updated:
                            to_create.append(d)
                        else:
                            modified_dt = normalize_datetime(d.get("modified"))
                            last_updated = normalize_datetime(existing_last_updated)

                            if last_updated < modified_dt:
                                to_update.append(d)

                    return to_update, to_create
                except Exception as e:
                    session.rollback()
                    print(f"Error: {e}")
                    raise e

        @staticmethod
        def add_astra_doc(docs, dump_date=None):
            """
            Populate the database with IDs already pushed to the database.
            """
            bulk_data = []
            for item in docs:
                vector = item.get('$vector')
                if hasattr(vector, "data"):
                    vector = vector.data
                vector_compressed = VectorType().process_bind_param(vector, None)

                bulk_data.append({
                    'id': item['_id'],
                    'vector': vector_compressed,
                    'lang': item['metadata']['Language'],
                    'wdid': item['metadata'].get('QID', item['metadata'].get('PID')),
                    'last_updated': item['metadata'].get('LastModified', datetime.utcnow().isoformat()),
                    'last_dump': dump_date,
                })

            VectorCache.add(bulk_data)

        @staticmethod
        def iter_batches(batch_size=1000):
            """
            Iterate vectors in deterministic batches.
            """
            with Session() as session:
                query = session.query(VectorCache).order_by(VectorCache.id).yield_per(batch_size)
                batch = []

                for row in query:
                    last_updated = row.last_updated
                    if isinstance(last_updated, datetime):
                        last_updated = last_updated.isoformat()

                    batch.append({
                        "id": row.id,
                        "vector": row.vector,
                        "lang": row.lang,
                        "wdid": row.wdid,
                        "last_updated": last_updated,
                    })

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                if batch:
                    yield batch


    # Create tables if they don't already exist.
    Base.metadata.create_all(engine)

    # Migrate existing tables: add last_dump column if absent.
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE vectors ADD COLUMN last_dump DATETIME"))
            conn.commit()
        except Exception:
            pass  # column already exists

    return VectorCache

class WikidataVectorCache:

    def __init__(self, lang='en', data_dir="../data/Wikidata/"):
        self.lang = lang
        self.data_dir = data_dir

        self.ItemVectorCache = get_db_connection(lang=lang, entity_type="items", data_dir=data_dir)
        self.PropertyVectorCache = get_db_connection(lang=lang, entity_type="properties", data_dir=data_dir)

    def add_astra_doc(self, docs, dump_date=None):
        if isinstance(dump_date, str):
            dump_date = normalize_datetime(dump_date)
        items = [doc for doc in docs if doc['_id'].startswith("Q")]
        if items:
            self.ItemVectorCache.add_astra_doc(items, dump_date=dump_date)
        properties = [doc for doc in docs if doc['_id'].startswith("P")]
        if properties:
            self.PropertyVectorCache.add_astra_doc(properties, dump_date=dump_date)

    def touch_last_dump(self, wdids, dump_date):
        if isinstance(dump_date, str):
            dump_date = normalize_datetime(dump_date)
        item_ids = [wdid for wdid in wdids if wdid.startswith("Q")]
        prop_ids = [wdid for wdid in wdids if wdid.startswith("P")]
        if item_ids:
            self.ItemVectorCache.touch_last_dump(item_ids, dump_date)
        if prop_ids:
            self.PropertyVectorCache.touch_last_dump(prop_ids, dump_date)

    def count_stale(self, dump_date):
        if isinstance(dump_date, str):
            dump_date = normalize_datetime(dump_date)
        return self.ItemVectorCache.count_stale(dump_date) + self.PropertyVectorCache.count_stale(dump_date)

    def iter_stale_batches(self, dump_date, batch_size=1000):
        if isinstance(dump_date, str):
            dump_date = normalize_datetime(dump_date)
        yield from self.ItemVectorCache.iter_stale_batches(dump_date, batch_size=batch_size)
        yield from self.PropertyVectorCache.iter_stale_batches(dump_date, batch_size=batch_size)

    def filter_for_update(self, data):
        """Filter out IDs that are already in the database and haven't been updated since the last push."""
        item_data = [d for d in data if d['id'].startswith("Q")]
        property_data = [d for d in data if d['id'].startswith("P")]

        if item_data:
            to_update_items, to_create_items = self.ItemVectorCache.filter_for_update(item_data)
        else:
            to_update_items = []
            to_create_items = []

        if property_data:
            to_update_properties, to_create_properties = self.PropertyVectorCache.filter_for_update(property_data)
        else:
            to_update_properties = []
            to_create_properties = []

        to_update = to_update_items + to_update_properties
        to_create = to_create_items + to_create_properties

        return to_update, to_create

    def iter_batches(self, batch_size=1000):
        """
        Iterate over all cached vectors (items first, then properties).
        """
        for batch in self.ItemVectorCache.iter_batches(batch_size=batch_size):
            yield batch

        for batch in self.PropertyVectorCache.iter_batches(batch_size=batch_size):
            yield batch
