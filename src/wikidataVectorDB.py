"""Wrap AstraDB vector collection operations used by the pipeline."""

import time
import traceback

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions, TimeoutOptions
from astrapy.exceptions.collection_exceptions import CollectionInsertManyException
from astrapy.exceptions.data_api_exceptions import DataAPIResponseException


class AstraDBConnect:
    """Connect to one language-specific AstraDB vector collection."""

    def __init__(
        self,
        lang="en",
        entity_type="items",
        application_token: str | None = None,
        api_endpoint: str | None = None,
        collection_prefix: str | None = None,
    ):
        """Initialize a connection to one AstraDB vector collection."""
        if not application_token:
            raise ValueError("Astra DB token not found.")
        if not api_endpoint:
            raise ValueError("Astra DB endpoint not found.")
        if not collection_prefix:
            raise ValueError("Astra collection prefix not found.")

        self.entity_type = entity_type
        self.lang = lang

        timeout_options = TimeoutOptions(request_timeout_ms=1000000)
        api_options = APIOptions(timeout_options=timeout_options)
        client = DataAPIClient(application_token, api_options=api_options)
        database0 = client.get_database(api_endpoint)

        collection_names = database0.list_collection_names()
        collection_name = f"{collection_prefix}_{entity_type}_{lang}"

        if collection_name in collection_names:
            self.collection = database0.get_collection(collection_name)
        else:
            raise ValueError(f"Collection {collection_name} not found in Astra DB.")

    def create_documents(self, docs):
        """Insert documents into AstraDB."""
        if len(docs) == 0:
            return []

        inserted_ids = []

        while True:
            try:
                result = self.collection.insert_many(docs)
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
                inserted_ids.extend(self.update_documents(docs))
                break
            except Exception:
                traceback.print_exc()
                time.sleep(1)

        return inserted_ids

    def update_documents(self, docs):
        """Update or upsert existing documents in AstraDB."""
        if len(docs) == 0:
            return []

        updated = []
        for doc in docs:
            docid = doc["_id"]
            update_fields = {key: value for key, value in doc.items() if key != "_id"}

            truncated = False
            while True:
                try:
                    self.collection.update_one(
                        filter={"_id": docid},
                        update={"$set": update_fields},
                        upsert=True,
                    )
                    updated.append(docid)
                    break
                except DataAPIResponseException as e:
                    # Content is too large to publish
                    if truncated:
                        raise e
                    update_fields["content"] = update_fields["content"][:3000] + " [TRUNCATED]"
                    truncated = True
                except Exception:
                    traceback.print_exc()
                    time.sleep(1)

        return updated

    def delete_documents(self, ids, batch_size=100):
        """Delete documents from AstraDB by ID and return the count deleted."""
        if not ids:
            return 0

        deleted = 0

        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            while True:
                try:
                    result = self.collection.delete_many({"_id": {"$in": batch}})
                    deleted += result.deleted_count
                    break
                except Exception:
                    traceback.print_exc()
                    time.sleep(1)

        return deleted
