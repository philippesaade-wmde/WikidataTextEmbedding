from sqlalchemy import Column, Text, DateTime, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import base64
import numpy as np
from datetime import datetime
from src.utils import normalize_last_updated

"""
SQLite database setup for logging pushed Wikidata IDs.
"""


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

        @staticmethod
        def add(data):
            """
            Bulk insert. Existing ones are updated.
            """
            with Session() as session:
                try:
                    for row in data:
                        row["last_updated"] = normalize_last_updated(row.get("last_updated"))

                    session.execute(
                        text("INSERT INTO vectors (id, vector, lang, wdid, last_updated) VALUES (:id, :vector, :lang, :wdid, :last_updated) ON CONFLICT(id) DO UPDATE SET vector = EXCLUDED.vector, lang = EXCLUDED.lang, wdid = EXCLUDED.wdid, last_updated = EXCLUDED.last_updated"),
                        data
                    )
                    session.commit()
                    return True
                except Exception as e:
                    session.rollback()
                    print(f"Error: {e}")
                    raise e

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
                            modified_dt = normalize_last_updated(d.get("modified"))
                            last_updated = normalize_last_updated(existing_last_updated)

                            if last_updated < modified_dt:
                                to_update.append(d)

                    return to_update, to_create
                except Exception as e:
                    session.rollback()
                    print(f"Error: {e}")
                    raise e

        @staticmethod
        def add_astra_doc(docs):
            """
            Populate the database with IDs already pushed to the database.

            Parameters:
            - graph_store (object): Object connection to Astra Database.
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
                    'last_updated': item['metadata'].get('LastModified', datetime.utcnow().isoformat())
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

    return VectorCache

class WikidataVectorCache:

    def __init__(self, lang='en', data_dir="../data/Wikidata/"):
        self.lang = lang
        self.data_dir = data_dir

        self.ItemVectorCache = get_db_connection(lang=lang, entity_type="items", data_dir=data_dir)
        self.PropertyVectorCache = get_db_connection(lang=lang, entity_type="properties", data_dir=data_dir)

    def add_astra_doc(self, docs):
        items = [
            doc for doc in docs \
                if doc['_id'].startswith("Q")
        ]
        if items:
            self.ItemVectorCache.add_astra_doc(items)

        properties = [
            doc for doc in docs \
                if doc['_id'].startswith("P")
        ]
        if properties:
            self.PropertyVectorCache.add_astra_doc(properties)

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
