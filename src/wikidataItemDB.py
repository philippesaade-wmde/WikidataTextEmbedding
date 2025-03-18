from sqlalchemy import Column, Text, String, Integer, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator, Boolean
import json
import re

"""
SQLite database setup for storing Wikidata labels & descriptions
in all languages.
"""

SQLITEDB_PATH = '../data/Wikidata/sqlite_wikidata_items.db'
engine = create_engine(f'sqlite:///{SQLITEDB_PATH}',
    pool_size=5,  # Limit the number of open connections
    max_overflow=10,  # Allow extra connections beyond pool_size
    pool_recycle=10  # Recycle connections every 10 seconds
)

Base = declarative_base()
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataItem(Base):
    """ Represents a Wikidata entity's labels in multiple languages."""

    __tablename__ = 'item'

    # TODO: convert ID to Integer and store existin IDs as qpid
    """
    id = Column(Integer, primary_key=True)
    qpid = Column(String, unique=True, index=True)
    """
    id = Column(Text, primary_key=True)
    labels = Column(JSONType)
    descriptions = Column(JSONType)
    in_wikipedia = Column(Boolean, default=False)

    @staticmethod
    def add_bulk_items(data):
        """
        Insert multiple label records in bulk. If a record with the same ID exists,
        it is ignored (no update is performed).

        Parameters:
        - data (list[dict]): A list of dictionaries, each containing 'id', 'labels', 'descriptions', and 'in_wikipedia' keys.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        worked = False  # Assume the operation failed
        with Session() as session:
            try:
                # Use a text statement to operate bulk insert
                # SQLAlchemy's ORM is unable to handle bulk inserts
                # with ON CONFLICT.

                insert_stmt = text(
                    """
                    INSERT INTO item (id, labels, descriptions, in_wikipedia)
                    VALUES (:id, :labels, :descriptions, :in_wikipedia)
                    ON CONFLICT(id) DO NOTHING
                    """
                )

                # Execute the insert statement for each data entry.
                session.execute(insert_stmt, data)
                session.commit()
                session.flush()
                worked = True  # Mark the operation as successful
            except Exception as e:
                session.rollback()
                print(e)

        return worked  # Return the operation status

    @staticmethod
    def add_labels(id, labels, descriptions, in_wikipedia):
        """
        Insert a single label record into the database.

        Parameters:
        - id (str): The unique identifier for the entity.
        - labels (dict): A dictionary of labels (e.g. { "en": "Label in English", "fr": "Label in French", ... }).

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                new_entry = WikidataItem(
                    id=id,
                    labels=labels,
                    descriptions=descriptions,
                    in_wikipedia=in_wikipedia
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
    def get_labels(id):
        """
        Retrieve labels for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        with Session() as session:
            item = session.query(WikidataItem).filter_by(id=id).first()
            if item is not None:
                return item.labels
            return {}

    @staticmethod
    def get_descriptions(id):
        """
        Retrieve labels for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        with Session() as session:
            item = session.query(WikidataItem).filter_by(id=id).first()
            if item is not None:
                return item.descriptions
            return {}

    @staticmethod
    def get_item(id):
        """
        Retrieve item for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        with Session() as session:
            item = session.query(WikidataItem).filter_by(id=id).first()
            if item is not None:
                return item
            return {}

    @staticmethod
    def clean_label_description(data):
        clean_data = {}
        for lang, label in data.items():
            clean_data[lang] = label['value']
        return clean_data

    @staticmethod
    def is_in_wikipedia(entity):
        """
        Check if a Wikidata entity has a corresponding Wikipedia entry in any language.

        Parameters:
        - entity (dict): A Wikidata entity dictionary.

        Returns:
        - bool: True if the entity has at least one sitelink ending in 'wiki', otherwise False.
        """
        if ('sitelinks' in entity):
            for s in entity['sitelinks']:
                if s.endswith('wiki'):
                    return True
        return False

    @staticmethod
    def get_labels_list(id_list):
        """
        Retrieve labels for multiple entities at once.

        Parameters:
        - id_list (list[str]): A list of entity IDs.

        Returns:
        - dict: A mapping of {entity_id: labels_dict} for each found ID. Missing IDs won't appear.
        """
        with Session() as session:
            rows = (
                session.query(WikidataItem.id, WikidataItem.labels)
                    .filter(WikidataItem.id.in_(id_list))
                    .all()
            )

        return {row_id: row_labels for row_id, row_labels in rows if row_labels is not None}

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
            data = {key: WikidataItem._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            data = [WikidataItem._remove_keys(item, keys_to_remove) for item in data]
        return data

    @staticmethod
    def _clean_datavalue(data):
        """
        Remove unnecessary nested structures unless they match a Wikidata entity or property pattern.

        Parameters:
        - data (dict or list): The data structure to clean.

        Returns:
        - dict or list: The cleaned data.
        """
        if isinstance(data, dict):
            # If there's only one key and it's not a property or QID, recurse into it.
            if (len(data.keys()) == 1) and not re.match(r"^[PQ]\d+$", list(data.keys())[0]):
                data = WikidataItem._clean_datavalue(data[list(data.keys())[0]])
            else:
                data = {key: WikidataItem._clean_datavalue(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [WikidataItem._clean_datavalue(item) for item in data]
        return data

    @staticmethod
    def _gather_labels_ids(data):
        """
        Find and return all relevant Wikidata IDs (e.g., property, unit, or datavalue IDs) in the claims.

        Parameters:
        - data (dict or list): The data structure to scan.

        Returns:
        - list[str]: A list of discovered Wikidata IDs.
        """
        ids = set()

        if isinstance(data, dict):
            if 'property' in data:
                ids.add(data['property'])

            if 'unit' in data and data['unit'] != '1':
                unit_id = data['unit'].split('/')[-1]
                ids.add(unit_id)

            datatype_in_data = 'datatype' in data
            datavalue_in_data = 'datavalue' in data
            data_datatype = data['datatype'] in (
                'wikibase-item', 'wikibase-property'
            )
            if datatype_in_data and datavalue_in_data and data_datatype:
                ids.add(data['datavalue'])

            for value in data.values():
                sub_ids = WikidataItem._gather_labels_ids(value)
                ids.update(sub_ids)

        elif isinstance(data, list):
            for item in data:
                sub_ids = WikidataItem._gather_labels_ids(item)
                ids.update(sub_ids)

        return list(ids)

    @staticmethod
    def _add_labels_to_claims(data, labels_dict={}):
        """
        For each found ID (property, unit, or datavalue) within the claims,
        insert the corresponding labels from labels_dict or the database.

        Parameters:
        - data (dict or list): The claims data structure.
        - labels_dict (dict): An optional dict of {id: labels} for quick lookup.

        Returns:
        - dict or list: The updated data with added label information.
        """
        if isinstance(data, dict):
            if 'property' in data:
                if data['property'] in labels_dict:
                    labels = labels_dict[data['property']]
                else:
                    labels = WikidataItem.get_labels(data['property'])

                data = {
                    **data,
                    'property-labels': labels
                }

            if ('unit' in data) and (data['unit'] != '1'):
                id = data['unit'].split('/')[-1]
                if id in labels_dict:
                    labels = labels_dict[id]
                else:
                    labels = WikidataItem.get_labels(id)

                data = {
                    **data,
                    'unit-labels': labels
                }

            if ('datatype' in data) and ('datavalue' in data) and ((data['datatype'] == 'wikibase-item') or (data['datatype'] == 'wikibase-property')):
                if data['datavalue'] in labels_dict:
                    labels = labels_dict[data['datavalue']]
                else:
                    labels = WikidataItem.get_labels(data['datavalue'])

                data['datavalue'] = {
                    'id': data['datavalue'],
                    'labels': labels
                }

            data = {key: WikidataItem._add_labels_to_claims(value, labels_dict=labels_dict) for key, value in data.items()}

        elif isinstance(data, list):
            data = [WikidataItem._add_labels_to_claims(item, labels_dict=labels_dict) for item in data]

        return data

    @staticmethod
    def add_labels_batched(claims, query_batch=100):
        """
        Gather all relevant IDs from claims, batch-fetch their labels, then add them to the claims structure.

        Parameters:
        - claims (dict or list): The claims data structure to update.
        - query_batch (int): The batch size for querying labels in groups. Default is 100.

        Returns:
        - dict or list: The updated claims with labels inserted.
        """
        label_ids = WikidataItem._gather_labels_ids(claims)

        labels_dict = {}
        for i in range(0, len(label_ids), query_batch):
            temp_dict = WikidataItem.get_labels_list(label_ids[i:i+query_batch])
            labels_dict = {**labels_dict, **temp_dict}

        claims = WikidataItem._add_labels_to_claims(claims, labels_dict=labels_dict)
        return claims

    @staticmethod
    def clean_entity(entity):
        """
        Clean a Wikidata entity's data by removing unneeded keys and adding label info to claims.

        Parameters:
        - entity (dict): A Wikidata entity dictionary containing 'claims', 'labels', 'sitelinks', etc.

        Returns:
        - dict: The cleaned entity with label data integrated into its claims.
        """
        clean_claims = WikidataItem._remove_keys(entity.get('claims', {}), ['hash', 'snaktype', 'type', 'entity-type', 'numeric-id', 'qualifiers-order', 'snaks-order'])
        clean_claims = WikidataItem._clean_datavalue(clean_claims)
        clean_claims = WikidataItem._remove_keys(clean_claims, ['id'])
        clean_claims = WikidataItem.add_labels_batched(clean_claims)

        sitelinks = WikidataItem._remove_keys(entity.get('sitelinks', {}), ['badges'])

        return {
            'id': entity['id'],
            'labels': WikidataItem.clean_label_description(entity['labels']),
            'descriptions': WikidataItem.clean_label_description(entity['descriptions']),
            'aliases': entity['aliases'],
            'sitelinks': sitelinks,
            'claims': clean_claims
        }

# Create tables if they don't already exist.
Base.metadata.create_all(engine)