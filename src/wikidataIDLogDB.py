from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"""
SQLite database setup for logging pushed Wikidata IDs.
"""

SQLITEDB_PATH = '../data/Wikidata/sqlite_wikidata_ids.db'
engine = create_engine(f'sqlite:///{SQLITEDB_PATH}',
    pool_size=5,  # Limit the number of open connections
    max_overflow=10,  # Allow extra connections beyond pool_size
    pool_recycle=10  # Recycle connections every 10 seconds
)

Base = declarative_base()
Session = sessionmaker(bind=engine)

class WikidataIDLog(Base):
    __tablename__ = 'id'
    id = Column(Text, primary_key=True)

    @staticmethod
    def add_id(id):
        """
        Insert an ID into the database.

        Parameters:
        - id (str): The unique identifier for the entity.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        with Session() as session:
            try:
                res = session.execute(
                    text("INSERT OR IGNORE INTO id(id) VALUES (:id)"),
                    {"id": id},
                )
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def is_pushed(id):
        """
        Returns whether the ID is found in the database, therefore is pushed to the Vector Database.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - bool: True if the ID exists in the database, False otherwise.
        """
        with Session() as session:
            id = session.query(WikidataIDLog).filter_by(id=id).first()
            if id is None:
                return False
            return True


# Create tables if they don't already exist.
Base.metadata.create_all(engine)