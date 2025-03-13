from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator

import os
import json

"""
SQLite database setup for caching the query embeddings for a faster
evaluation process.
"""

# TODO: Move to a configuration file
wikidata_cache_file = "wikidata_cache.db"

wikidata_cache_dir = "../data/Wikidata"
wikidata_cache_path = os.path.join(wikidata_cache_dir, wikidata_cache_file)

try:
    if not os.path.exists(wikidata_cache_dir):
        os.makedirs(wikidata_cache_dir)
except OSError as e:
    print(f"Error creating directory {wikidata_cache_dir}: {e}")

assert os.path.exists(wikidata_cache_dir), \
    f"Error creating directory {wikidata_cache_dir}"

engine = create_engine(
    f'sqlite:///{wikidata_cache_path}',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10    # Recycle connections every 10 seconds
)

Base = declarative_base()
Session = sessionmaker(bind=engine)


class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None


def create_cache_embedding_model(table_name):
    """Factory function to create a dynamic CacheEmbeddings model."""

    class CacheEmbeddings(Base):
        __tablename__ = table_name

        id = Column(Text, primary_key=True)
        embedding = Column(JSONType)

        @staticmethod
        def add_cache(id, embedding):
            with Session() as session:
                try:
                    cached = CacheEmbeddings(id=id, embedding=embedding)
                    session.merge(cached)
                    session.commit()
                    return True
                except Exception as e:
                    session.rollback()
                    raise e

        @staticmethod
        def get_cache(id):
            with Session() as session:
                cached = session.query(
                    CacheEmbeddings
                ).filter_by(id=id).first()

                if cached:
                    return cached.embedding
                return None

        @staticmethod
        def add_bulk_cache(data):
            """
            Insert multiple label records in bulk. If a record with the same
            ID exists,
            it is ignored (no update is performed).

            Parameters:
            - data (list[dict]): A list of dictionaries, each containing 'id',
            'labels', 'descriptions', and 'in_wikipedia' keys.

            Returns:
            - bool: True if the operation was successful, False otherwise.
            """
            worked = False
            with Session() as session:
                exec_text = text(
                    f"""
                    INSERT INTO {CacheEmbeddings.__tablename__} (id, embedding)
                    VALUES (:id, :embedding)
                    ON CONFLICT(id) DO NOTHING
                    """
                )
                try:
                    session.execute(exec_text, data)
                    session.commit()
                    session.flush()
                    worked = True
                except Exception as e:
                    session.rollback()
                    print(e)
            return worked

    Base.metadata.create_all(engine)

    return CacheEmbeddings
