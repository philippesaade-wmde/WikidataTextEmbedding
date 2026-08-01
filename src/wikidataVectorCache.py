"""SQLite cache for Wikidata vector embeddings pushed to AstraDB."""

import base64
from datetime import datetime

import numpy as np
from sqlalchemy import Column, DateTime, Index, Text, create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator

from src.utils import normalize_datetime


class VectorType(TypeDecorator):
    """Custom SQLAlchemy type for Vector storage in SQLite."""

    impl = Text
    cache_ok = False

    def process_bind_param(self, value, dialect):
        """Encode a vector list as base64 float32 bytes."""
        arr = np.array(value, dtype="<f4")
        binary_data = arr.tobytes()
        return base64.b64encode(binary_data).decode("utf8")

    def process_result_value(self, vector, dialect):
        """Decode base64 float32 bytes into a vector list."""
        binary_data = base64.b64decode(vector)
        embedding_array = np.frombuffer(binary_data, dtype="<f4")
        return embedding_array.tolist()


def get_db_connection(lang="en", entity_type="items", data_dir="../data/Wikidata"):
    """Create the SQLite vector cache model for one language and entity type."""
    db_name = f"sqlite_wikidata_vectors_{entity_type}_{lang}.db"
    engine = create_engine(
        f"sqlite:///{data_dir}/{db_name}",
        pool_size=5,  # Limit the number of open connections
        max_overflow=10,  # Allow extra connections beyond pool_size
        pool_recycle=10,  # Recycle connections every 10 seconds
        connect_args={"timeout": 10_000},
    )

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class VectorCache(Base):
        __tablename__ = "vectors"
        __table_args__ = (
            Index("idx_vectors_wdid", "wdid"),
            Index("idx_vectors_last_dump_id", "last_dump", "id"),
        )

        id = Column(Text, primary_key=True)
        vector = Column(VectorType)
        lang = Column(Text)
        wdid = Column(Text)
        last_updated = Column(DateTime, default=datetime.utcnow)
        last_dump = Column(DateTime, nullable=True)

        @staticmethod
        def add(data):
            """Bulk insert rows and update existing ones."""
            with Session() as session:
                try:
                    for row in data:
                        row["last_updated"] = normalize_datetime(row.get("last_updated"))

                    session.execute(
                        text(
                            "INSERT INTO vectors "
                            "(id, vector, lang, wdid, last_updated, last_dump) "
                            "VALUES (:id, :vector, :lang, :wdid, "
                            ":last_updated, :last_dump) "
                            "ON CONFLICT(id) DO UPDATE SET "
                            "vector = EXCLUDED.vector, "
                            "lang = EXCLUDED.lang, "
                            "wdid = EXCLUDED.wdid, "
                            "last_updated = EXCLUDED.last_updated, "
                            "last_dump = EXCLUDED.last_dump"
                        ),
                        data,
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
            if isinstance(dump_date, str):
                dump_date = normalize_datetime(dump_date)
            with Session() as session:
                session.query(VectorCache).filter(VectorCache.wdid.in_(wdids)).update(
                    {"last_dump": dump_date}, synchronize_session=False
                )
                session.commit()

        @staticmethod
        def count_stale(dump_date):
            """Return the number of rows with last_dump older than dump_date or NULL."""
            if isinstance(dump_date, str):
                dump_date = normalize_datetime(dump_date)
            with Session() as session:
                return session.execute(
                    text("SELECT COUNT(*) FROM vectors WHERE last_dump IS NULL OR last_dump < :d"),
                    {"d": dump_date},
                ).scalar()

        @staticmethod
        def iter_stale_batches(dump_date, batch_size=1000):
            """Yield batches of stale row IDs, deleting each batch from SQLite after the caller resumes."""
            if isinstance(dump_date, str):
                dump_date = normalize_datetime(dump_date)
            cursor = ""
            while True:
                with Session() as session:
                    batch = (
                        session.execute(
                            text(
                                "SELECT id FROM vectors "
                                "WHERE (last_dump IS NULL OR last_dump < :d) "
                                "AND id > :c "
                                "ORDER BY id LIMIT :n"
                            ),
                            {"d": dump_date, "c": cursor, "n": batch_size},
                        )
                        .scalars()
                        .all()
                    )
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
                        session.query(VectorCache.wdid, VectorCache.last_updated).filter(VectorCache.id.in_(ids)).all()
                    )
                    existing_dict = {wdid: last_updated for wdid, last_updated in existing_rows if wdid}

                    to_update = []
                    to_create = []
                    for d in data:
                        existing_last_updated = existing_dict.get(d["id"])
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
            """Populate the database with IDs already pushed to the database."""
            if isinstance(dump_date, str):
                dump_date = normalize_datetime(dump_date)
            bulk_data = []
            for item in docs:
                vector = item.get("$vector")
                if hasattr(vector, "data"):
                    vector = vector.data
                vector_compressed = VectorType().process_bind_param(vector, None)

                bulk_data.append(
                    {
                        "id": item["_id"],
                        "vector": vector_compressed,
                        "lang": item["metadata"]["Language"],
                        "wdid": item["metadata"].get("QID", item["metadata"].get("PID")),
                        "last_updated": item["metadata"].get("LastModified", datetime.utcnow().isoformat()),
                        "last_dump": dump_date,
                    }
                )

            VectorCache.add(bulk_data)

        @staticmethod
        def iter_batches(batch_size=1000):
            """Iterate vectors in deterministic batches."""
            cursor = ""
            while True:
                with Session() as session:
                    rows = (
                        session.execute(
                            text(
                                "SELECT id, vector, lang, wdid, last_updated "
                                "FROM vectors WHERE id > :cursor ORDER BY id LIMIT :limit"
                            ),
                            {"cursor": cursor, "limit": batch_size},
                        )
                        .mappings()
                        .all()
                    )

                if not rows:
                    break

                batch = []
                for row in rows:
                    last_updated = row["last_updated"]
                    if isinstance(last_updated, datetime):
                        last_updated = last_updated.isoformat()

                    batch.append(
                        {
                            "id": row["id"],
                            "vector": row["vector"],
                            "lang": row["lang"],
                            "wdid": row["wdid"],
                            "last_updated": last_updated,
                        }
                    )

                yield batch
                cursor = batch[-1]["id"]

    # Multiple workers can initialize a new cache at once. One may create the
    # table after another worker has already passed SQLAlchemy's checkfirst.
    try:
        Base.metadata.create_all(engine)
    except OperationalError as exc:
        if "already exists" not in str(exc).lower():
            raise

    # Migrate existing tables: add last_dump column if absent.
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE vectors ADD COLUMN last_dump DATETIME"))
            conn.commit()
        except OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise

    return VectorCache
