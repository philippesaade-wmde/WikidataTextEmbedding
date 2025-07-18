from sqlalchemy import Column, Text, String, Integer, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator, Boolean
import json
import re
import traceback

"""
SQLite database setup for storing Wikidata labels & descriptions
in all languages.
"""

SQLITEDB_PATH = '../data/Wikidata/sqlite_wikidata_entities.db'
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
    __tablename__ = 'item'
    id = Column(Text, primary_key=True)
    labels = Column(JSONType)
    descriptions = Column(JSONType)
    in_wikipedia = Column(Boolean, default=False)

class WikidataProperty(Base):
    __tablename__ = 'property'
    id = Column(Text, primary_key=True)
    labels = Column(JSONType)
    descriptions = Column(JSONType)
    in_wikipedia = Column(Boolean, default=False)

    property_filter = Column(String, default='full') # values: 'full', 'remove', 'has'
    property_sort = Column(Integer, default=90000)


class WikidataEntity:
    """ Represents a Wikidata entity's labels in multiple languages."""

    @staticmethod
    def _get_class(id):
        """
        Returns the appropriate class based on the entity ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - WikidataItem or WikidataProperty: The class corresponding to the entity type.
        """
        return WikidataProperty if id.startswith('P') else WikidataItem

    @staticmethod
    def add_bulk_items(data):
        """
        Insert multiple label records in bulk. If a record with the same ID exists, it is ignored (no update is performed).

        Parameters:
        - data (list[dict]): A list of dictionaries, each containing 'id', 'labels', 'descriptions', and 'in_wikipedia' keys.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        with Session() as session:
            try:
                items = [d for d in data if d['id'].startswith('Q')]
                properties = [d for d in data if d['id'].startswith('P')]

                if items:
                    session.execute(text('''
                        INSERT INTO wikidata_item (id, labels, descriptions, in_wikipedia)
                        VALUES (:id, :labels, :descriptions, :in_wikipedia)
                        ON CONFLICT(id) DO NOTHING
                    '''), items)

                if properties:
                    session.execute(text('''
                        INSERT INTO wikidata_property (id, labels, descriptions, in_wikipedia)
                        VALUES (:id, :labels, :descriptions, :in_wikipedia)
                        ON CONFLICT(id) DO NOTHING
                    '''), properties)

                session.commit()
                return True
            except Exception as e:
                session.rollback()
                traceback.print_exc()
                return False

    @staticmethod
    def add_entity(id, labels, descriptions, in_wikipedia):
        """
        Insert a labels and descriptions into the database.

        Parameters:
        - id (str): The unique identifier for the entity.
        - labels (dict): A dictionary of labels (e.g. { "en": "Label in English", "fr": "Label in French", ... }).
        - descriptions (dict): A dictionary of descriptions (e.g. { "en": "Description in English", "fr": "Description in French", ... }).

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        cls = WikidataEntity._get_class(id)
        with Session() as session:
            try:
                new_entry = cls(
                    id=id,
                    labels=labels,
                    descriptions=descriptions,
                    in_wikipedia=in_wikipedia
                )
                session.add(new_entry)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
                return False

    @staticmethod
    def get_labels(id):
        """
        Retrieve labels and descriptions for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        cls = WikidataEntity._get_class(id)
        with Session() as session:
            item = session.query(cls).filter_by(id=id).first()
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
        cls = WikidataEntity._get_class(id)
        with Session() as session:
            item = session.query(cls).filter_by(id=id).first()
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
        cls = WikidataEntity._get_class(id)
        with Session() as session:
            item = session.query(cls).filter_by(id=id).first()
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
        items_ids = [id for id in id_list if id.startswith('Q')]
        props_ids = [id for id in id_list if id.startswith('P')]
        result = {}

        with Session() as session:
            if items_ids:
                rows = session.query(WikidataItem.id, WikidataItem.labels).filter(WikidataItem.id.in_(items_ids)).all()
                result.update({id: labels for id, labels in rows})

            if props_ids:
                rows = session.query(WikidataProperty.id, WikidataProperty.labels).filter(WikidataProperty.id.in_(props_ids)).all()
                result.update({id: labels for id, labels in rows})

        return result

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
            data = {key: WikidataEntity._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            data = [WikidataEntity._remove_keys(item, keys_to_remove) for item in data]
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
                data = WikidataEntity._clean_datavalue(data[list(data.keys())[0]])
            else:
                data = {key: WikidataEntity._clean_datavalue(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [WikidataEntity._clean_datavalue(item) for item in data]
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
                sub_ids = WikidataEntity._gather_labels_ids(value)
                ids.update(sub_ids)

        elif isinstance(data, list):
            for item in data:
                sub_ids = WikidataEntity._gather_labels_ids(item)
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
                    labels = WikidataEntity.get_labels(data['property'])

                data = {
                    **data,
                    'property-labels': labels
                }

            if ('unit' in data) and (data['unit'] != '1'):
                id = data['unit'].split('/')[-1]
                if id in labels_dict:
                    labels = labels_dict[id]
                else:
                    labels = WikidataEntity.get_labels(id)

                data = {
                    **data,
                    'unit-labels': labels
                }

            if ('datatype' in data) and ('datavalue' in data) and \
                (data['datatype'] in ['wikibase-item', 'wikibase-property']):
                if data['datavalue'] in labels_dict:
                    labels = labels_dict[data['datavalue']]
                else:
                    labels = WikidataEntity.get_labels(data['datavalue'])

                data['datavalue'] = {
                    'id': data['datavalue'],
                    'labels': labels
                }

            data = {key: WikidataEntity._add_labels_to_claims(value, labels_dict=labels_dict) for key, value in data.items()}

        elif isinstance(data, list):
            data = [WikidataEntity._add_labels_to_claims(item, labels_dict=labels_dict) for item in data]

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
        label_ids = WikidataEntity._gather_labels_ids(claims)

        labels_dict = {}
        for i in range(0, len(label_ids), query_batch):
            temp_dict = WikidataEntity.get_labels_list(label_ids[i:i+query_batch])
            labels_dict = {**labels_dict, **temp_dict}

        claims = WikidataEntity._add_labels_to_claims(claims, labels_dict=labels_dict)
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
        clean_claims = WikidataEntity._remove_keys(entity.get('claims', {}), ['hash', 'snaktype', 'type', 'entity-type', 'numeric-id', 'qualifiers-order', 'snaks-order'])
        clean_claims = WikidataEntity._clean_datavalue(clean_claims)
        clean_claims = WikidataEntity._remove_keys(clean_claims, ['id'])
        clean_claims = WikidataEntity.add_labels_batched(clean_claims)

        sitelinks = WikidataEntity._remove_keys(entity.get('sitelinks', {}), ['badges'])

        return {
            'id': entity['id'],
            'labels': WikidataEntity.clean_label_description(
                entity['labels']
            ),
            'descriptions': WikidataEntity.clean_label_description(
                entity['descriptions']
            ),
            'aliases': entity['aliases'],
            'sitelinks': sitelinks,
            'claims': clean_claims
        }

# Create tables if they don't already exist.
Base.metadata.create_all(engine)