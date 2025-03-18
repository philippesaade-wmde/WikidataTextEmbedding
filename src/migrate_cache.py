import sqlite3
import json
import base64
import numpy as np
from tqdm import tqdm

# Change this to match your actual database path
DB_PATH = "data/Wikidata/wikidata_cache.db"

TABLE_NAME = "wikidata_prototype"  # Change this to match your actual table name
BATCH_SIZE = 5000  # Process in smaller batches to avoid memory overload

def convert_embeddings():
    """
    Convert JSON-stored embeddings into Base64-encoded binary format in batches.
    Uses `fetchmany(BATCH_SIZE)` to process records iteratively.
    """
    # TODO: Migrate away from global variables
    print(f"Converting embeddings in {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check if the embedding column exists (sanity check)
    cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
    columns = [row[1] for row in cursor.fetchall()]
    if "embedding" not in columns:
        print("Error: 'embedding' column does not exist in the table!")
        return

    # Count total records for progress tracking
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total_records = cursor.fetchone()[0]

    print(f"Total records to process: {total_records}")

    # Fetch records in batches using an iterator
    offset = 0
    with tqdm(total=total_records, desc="Converting embeddings", unit="record") as pbar:
        while True:
            cursor.execute(f"SELECT id, embedding FROM {TABLE_NAME} LIMIT {BATCH_SIZE} OFFSET {offset}")
            records = cursor.fetchall()
            if not records:
                break  # Stop when there are no more records

            updated_records = []
            for id, json_embedding in records:
                if json_embedding:
                    try:
                        # Convert JSON string to list of floats
                        embedding_list = json.loads(json_embedding)

                        # Convert list of floats to Base64-encoded binary
                        binary_data = np.array(embedding_list, dtype=np.float32).tobytes()
                        base64_embedding = base64.b64encode(binary_data).decode('utf-8')

                        updated_records.append((base64_embedding, id))
                    except Exception as e:
                        pass

                pbar.update(1)  # Update progress bar for each record processed

            # Update database in batches
            if updated_records:
                cursor.executemany(
                    f"UPDATE {TABLE_NAME} SET embedding = ? WHERE id = ?",
                    updated_records
                )
                conn.commit()  # Commit every batch

            offset += BATCH_SIZE  # Move to next batch

    print("Optimizing database with VACUUM...")
    cursor.execute("VACUUM;")
    conn.commit()

    print("Migration completed successfully.")

    conn.close()

if __name__ == "__main__":
    convert_embeddings()