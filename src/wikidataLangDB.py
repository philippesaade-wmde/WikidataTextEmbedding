from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import os

"""
SQLite database setup for quick entity lookup. A database file is created per language.
"""
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

def create_wikidatalang_db(
        db_filname,
        table_name='wikidata'
    ):
    """Factory function to create a dynamic WikidataLang model."""

    data_dir = os.path.abspath("../data/Wikidata")
    sqldb_path = os.path.join(data_dir, db_filname)

    # Safe directory creation if it's missing
    os.makedirs(data_dir, exist_ok=True)

    engine = create_engine(
        f"sqlite:///{sqldb_path}",
        pool_size=5,       # Limit the number of open connections
        max_overflow=10,   # Allow extra connections beyond pool_size
        pool_recycle=10    # Recycle connections every 10 seconds
    )
    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class WikidataLang(Base):
        """Represents a Wikidata entity with label, description, aliases, and claims."""

        __tablename__ = table_name

        id = Column(Text, primary_key=True)
        label = Column(Text)
        description = Column(Text)
        aliases = Column(JSONType)
        claims = Column(JSONType)

        @staticmethod
        def get_session():
            """
            Get a new session from the scoped session factory.

            Returns:
            - Session: A SQLAlchemy session object.
            """
            return Session()

        @staticmethod
        def add_bulk_entities(data):
            """
            Add multiple entities to the database in bulk, if the item already exists then it's ignored.

            Parameters:
            - data (list[dict]): A list of dictionaries representing entities to be added.

            Returns:
            - bool: True if the operation was successful, False otherwise.
            """
            worked = False
            with Session() as session:
                try:
                    session.execute(
                        text(
                            """
                            INSERT INTO wikidata (id, label, description, claims, aliases)
                            VALUES (:id, :label, :description, :claims, :aliases)
                            ON CONFLICT(id) DO NOTHING
                            """
                        ),
                        data
                    )
                    session.commit()
                    session.flush()
                    worked = True
                except Exception as e:
                    session.rollback()
                    print(e)
            return worked

        @staticmethod
        def add_entity(id, label, description, claims, aliases):
            """
            Add a single Wikidata entity to the database.

            Parameters:
            - id (str): The unique identifier for the entity.
            - label (str): The entity's label.
            - description (str): The entity's description.
            - claims (dict): The entity's claims.
            - aliases (dict): The entity's aliases.

            Returns:
            - bool: True if successful, False otherwise.
            """
            worked = False
            with Session() as session:
                try:
                    new_entry = WikidataLang(
                        id=id,
                        label=label,
                        description=description,
                        claims=claims,
                        aliases=aliases
                    )
                    session.add(new_entry)
                    session.commit()
                    session.flush()
                    worked = True
                except Exception as e:
                    session.rollback()
                    print(f"Error: {e}")
            return worked

        @staticmethod
        def get_entity(id):
            """
            Retrieve an entity by its ID.

            Parameters:
            - id (str): The unique identifier of the entity.

            Returns:
            - WikidataEntity or None: The entity object if found, otherwise None.
            """
            with Session() as session:
                return session.query(WikidataLang).filter_by(id=id).first()

        @staticmethod
        def is_in_wikipedia(item, language='en'):
            """
            Check if a Wikidata item has a corresponding Wikipedia entry.

            Parameters:
            - item (dict): The Wikidata item.
            - language (str): The Wikipedia language code. Default is 'en'.

            Returns:
            - bool: True if the item has a Wikipedia sitelink and label/description in the specified language or 'mul'.
            """
            condition = ('sitelinks' in item) and (f'{language}wiki' in item['sitelinks']) # Has an Wikipedia Sitelink
            condition = condition and ((language in item['labels']) or ('mul' in item['labels'])) # Has a label with the corresponding language or multiligual
            condition = condition and ((language in item['descriptions']) or ('mul' in item['descriptions'])) # Has a description with the corresponding language or multiligual
            return condition

        @staticmethod
        def normalise_item(item, language='en'):
            """
            Normalize a Wikidata item into a dictionary for storage.

            Parameters:
            - item (dict): The raw Wikidata item.
            - language (str): The language code to use for label/description lookup. Default is 'en'.

            Returns:
            - dict: A dictionary containing normalized entity data suitable for insertion.
            """
            label = item['labels'][language]['value'] if (language in item['labels']) else (item['labels']['mul']['value'] if ('mul' in item['labels']) else '') # Take the label from the language, if missing take it from the multiligual class
            description = item['descriptions'][language]['value'] if (language in item['descriptions']) else (item['descriptions']['mul']['value'] if ('mul' in item['descriptions']) else '') # Take the description from the language, if missing take it from the multiligual class
            aliases = WikidataLang._get_aliases(item, language=language)
            claims = WikidataLang._get_claims(item)
            return {
                'id': item['id'],
                'label': label,
                'description': description,
                'aliases': json.dumps(aliases, separators=(',', ':')),
                'claims': json.dumps(claims, separators=(',', ':')),
            }

        @staticmethod
        def _remove_keys(data, keys_to_remove=['hash', 'property', 'numeric-id', 'qualifiers-order']):
            """
            Recursively remove specific keys from a nested data structure.

            Parameters:
            - data (dict or list): The data structure to clean.
            - keys_to_remove (list): Keys to remove. Default includes 'hash', 'property', 'numeric-id', and 'qualifiers-order'.

            Returns:
            - dict or list: The cleaned data structure with specified keys removed.
            """
            if isinstance(data, dict):
                return {key: WikidataLang._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
            elif isinstance(data, list):
                return [WikidataLang._remove_keys(item, keys_to_remove) for item in data]
            else:
                return data

        @staticmethod
        def _get_claims(item):
            """
            Extract and clean claims from a Wikidata item.

            Parameters:
            - item (dict): The raw Wikidata item.

            Returns:
            - dict: A dictionary of extracted claims, keyed by property ID.
            """
            claims = {}
            if 'claims' in item:
                for pid,x in item['claims'].items():
                    pid_claims = []
                    for i in x:
                        if (i['type'] == 'statement') and (i['rank'] != 'deprecated'):
                            pid_claims.append({
                                'mainsnak': WikidataLang._remove_keys(i['mainsnak']) if 'mainsnak' in i else {},
                                'qualifiers': WikidataLang._remove_keys(i['qualifiers']) if 'qualifiers' in i else {},
                                'rank': i['rank']
                            })
                    if len(pid_claims) > 0:
                        claims[pid] = pid_claims
            return claims

        @staticmethod
        def _get_aliases(item, language='en'):
            """
            Extract aliases from a Wikidata item for a given language, plus any 'mul' entries.

            Parameters:
            - item (dict): The raw Wikidata item.
            - language (str): The language code. Default is 'en'.

            Returns:
            - list[str]: A list of aliases in the specified language (and 'mul' if present).
            """
            aliases = set()
            if language in item['aliases']:
                aliases = set([x['value'] for x in item['aliases'][language]])
            if 'mul' in item['aliases']:
                aliases = aliases | set([x['value'] for x in item['aliases']['mul']])
            return list(aliases)

    # Create tables if they don't already exist.
    Base.metadata.create_all(engine)
    return WikidataLang