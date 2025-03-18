from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator

import os
import base64
import numpy as np

"""
SQLite database setup for caching the query embeddings for a faster
evaluation process.
"""

class EmbeddingType(TypeDecorator):
    """Custom SQLAlchemy type for storing embeddings as Base64 strings in SQLite."""

    impl = Text

    def process_bind_param(self, value, dialect):
        """Convert a list of floats (embedding) to a Base64 string before storing."""
        if value is not None and isinstance(value, list):
            # Convert list to binary
            binary_data = np.array(value, dtype=np.float32).tobytes()
            # Encode to Base64 string
            return base64.b64encode(binary_data).decode('utf-8')
        return None

    def process_result_value(self, value, dialect):
        """Convert a Base64 string back to a list of floats when retrieving."""
        if value is not None:
            # Decode Base64
            binary_data = base64.b64decode(value)
            # Convert back to float32 list
            embedding_array = np.frombuffer(binary_data, dtype=np.float32)
            return embedding_array.tolist()
        return None

def create_cache_embedding_db(
        db_filname="wikidata_cache.db",
        table_name="wikidata_prototype"
    ):
    """Factory function to create a dynamic CacheEmbeddings model."""

    wikidata_cache_dir = os.path.abspath("../data/Wikidata")
    wikidata_cache_path = os.path.join(wikidata_cache_dir, db_filname)

    # Safe directory creation if it's missing
    os.makedirs(wikidata_cache_dir, exist_ok=True)

    engine = create_engine(
        f'sqlite:///{wikidata_cache_path}',
        pool_size=5,       # Limit the number of open connections
        max_overflow=10,   # Allow extra connections beyond pool_size
        pool_recycle=10    # Recycle connections every 10 seconds
    )

    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class CacheEmbeddings(Base):
        __tablename__ = table_name

        id = Column(Text, primary_key=True)
        embedding = Column(EmbeddingType)

        @staticmethod
        def get_session():
            """
            Get a new session from the scoped session factory.

            Returns:
            - Session: A SQLAlchemy session object.
            """
            return Session()

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
            embeddingtype = EmbeddingType()
            for i in range(len(data)):
                data[i]['embedding'] = embeddingtype.process_bind_param(
                    data[i]['embedding'],
                    None
                )

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
