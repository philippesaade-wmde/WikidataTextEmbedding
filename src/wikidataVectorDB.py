import time
import traceback
import os
from astrapy import DataAPIClient
from astrapy.exceptions.collection_exceptions import CollectionInsertManyException
from astrapy.exceptions.data_api_exceptions import DataAPIResponseException
from astrapy.api_options import APIOptions, TimeoutOptions

import json

class AstraDBConnect:
    def __init__(
            self,
            lang="en",
            entity_type="items",
            config_path: str = "../API_tokens/datastax_api.json"):
        """
        Initialize the AstraDBConnect object with the corresponding embedding model.

        Parameters:
        - datastax_token (dict): Credentials for DataStax Astra, including token and API endpoint.
        - collection_name (str): Name of the collection (table) where data is stored.
        - model (str): The embedding model to use. Default is 'jina'.
        - batch_size (int): Number of documents to accumul
        ate before pushing to AstraDB. Default is 8.
        """
        datastax_token = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f_in:
                datastax_token = json.load(f_in)

        ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
        ASTRA_DB_API_ENDPOINT = os.environ.get("ASTRA_DB_API_ENDPOINT")
        collection_prefix = os.environ.get("ASTRA_COLLECTION_PREFIX")

        ASTRA_DB_APPLICATION_TOKEN = datastax_token.get(
            "ASTRA_DB_APPLICATION_TOKEN",
            ASTRA_DB_APPLICATION_TOKEN
        )
        ASTRA_DB_API_ENDPOINT = datastax_token.get(
            "ASTRA_DB_API_ENDPOINT",
            ASTRA_DB_API_ENDPOINT
        )
        collection_prefix = datastax_token.get(
            "COLLECTION_PREFIX",
            collection_prefix
        )

        if not ASTRA_DB_APPLICATION_TOKEN:
            raise ValueError("Astra DB token not found.")
        if not ASTRA_DB_API_ENDPOINT:
            raise ValueError("Astra DB endpoint not found.")

        self.entity_type = entity_type
        self.lang = lang

        timeout_options = TimeoutOptions(request_timeout_ms=1000000)
        api_options = APIOptions(timeout_options=timeout_options)
        client = DataAPIClient(
            ASTRA_DB_APPLICATION_TOKEN,
            api_options=api_options
        )
        database0 = client.get_database(ASTRA_DB_API_ENDPOINT)

        collection_names = database0.list_collection_names()
        item_collection_name = f"{collection_prefix}_items_{lang}"
        property_collection_name = f"{collection_prefix}_properties_{lang}"

        if item_collection_name in collection_names:
            self.item_collection = database0.get_collection(
                f"{collection_prefix}_items_{lang}"
            )
        else:
            raise ValueError(f"Collection {item_collection_name} not found in Astra DB.")

        if property_collection_name in collection_names:
            self.property_collection = database0.get_collection(
                f"{collection_prefix}_properties_{lang}"
            )
        else:
            raise ValueError(f"Collection {property_collection_name} not found in Astra DB.")

    def create_documents(self, docs):
        """
        Push the current batch of documents to AstraDB for storage.

        doc = {
            '_id': str,
            'content': str,
            '$vector': list[float],
            'metadata': {
                "Label": str,
                "Description": str,
                "Date": str,
                "QID": str,
                "ChunkID": int,
                "Language": str,
                "InstanceOf": list[str],
                "Properties": list[str],
                "DumpDate": str
            }
        }
        """
        if len(docs) == 0:
            return []

        items = [
            doc for doc in docs \
                if doc['_id'].startswith("Q")
        ]
        properties = [
            doc for doc in docs \
                if doc['_id'].startswith("P")
        ]

        inserted_ids = []

        if items:
            while True:
                try:
                    result = self.item_collection.insert_many(items)
                    inserted_ids.extend(result.inserted_ids)
                    break
                except CollectionInsertManyException as e:
                    # Ignore duplicate IDs error.
                    traceback.print_exc()
                    inserted_ids.extend(e.inserted_ids)
                    break
                except DataAPIResponseException:
                    # Data is too large to publish in Bulk
                    traceback.print_exc()
                    inserted_ids.extend(self.update_documents(items))
                    break
                except Exception:
                    traceback.print_exc()
                    time.sleep(1)

        if properties:
            while True:
                try:
                    result = self.property_collection.insert_many(properties)
                    inserted_ids.extend(result.inserted_ids)
                    break
                except CollectionInsertManyException as e:
                    # Ignore duplicate IDs error.
                    traceback.print_exc()
                    inserted_ids.extend(e.inserted_ids)
                    break
                except DataAPIResponseException:
                    # Data is too large to publish in Bulk
                    inserted_ids.extend(self.update_documents(properties))
                    break
                except Exception:
                    traceback.print_exc()
                    time.sleep(1)

        return inserted_ids

    def update_documents(self, docs):
        """
        Update existing documents in AstraDB.

        doc = {
            '_id': str,
            'content': str,
            '$vector': list[float],
            'metadata': {
                "Label": str,
                "Description": str,
                "Date": str,
                "QID": str,
                "ChunkID": int,
                "Language": str,
                "InstanceOf": list[str],
                "Properties": list[str],
                "DumpDate": str
            }
        }
        """
        if len(docs) == 0:
            return []

        updated = []
        for doc in docs:
            docid = doc["_id"]
            update_fields = {
                key: value
                for key, value in doc.items()
                if key != "_id"
            }

            if docid.startswith("Q"):
                collection = self.item_collection
            elif docid.startswith("P"):
                collection = self.property_collection
            else:
                continue

            truncated = False
            while True:
                try:
                    collection.update_one(
                        filter={"_id": docid},
                        update={
                            "$set": update_fields
                        },
                        upsert=True
                    )
                    updated.append(docid)
                    break
                except DataAPIResponseException as e:
                    # Content is too large to publish
                    if truncated:
                        raise e
                    update_fields['content'] = update_fields['content'][:3000] + " [TRUNCATED]"
                    truncated = True
                except Exception:
                    traceback.print_exc()
                    time.sleep(1)

        return updated
