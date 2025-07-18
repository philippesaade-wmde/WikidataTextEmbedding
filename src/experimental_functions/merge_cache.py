import sqlite3
import glob
import os
import base64
from tqdm import tqdm
import json
import numpy as np

# Define database file pattern
db_files = ["../../data/Wikidata/wikidata_cache_chunk0-30_18-9-2024.db"]

# Define the target merged database
merged_db = "../../data/Wikidata/wikidata_cache.db"
TABLE_NAME = "wikidata_prototype"

# Batch size for processing
BATCH_SIZE = 5000  # Adjust based on performance needs

# Create the merged database connection
conn_merged = sqlite3.connect(merged_db)
cursor_merged = conn_merged.cursor()

# Helper function to check if a string is a valid Base64 encoding
def is_valid_base64(s):
    try:
        if not s or not isinstance(s, str):
            return False
        base64.b64decode(s, validate=True)
        return True
    except Exception:
        return False

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except:
        return False

# Loop through all source databases
for db_file in db_files:
    print(f"Processing {db_file}...")

    # Connect to the current database
    conn_src = sqlite3.connect(db_file)
    cursor_src = conn_src.cursor()

    # Get total record count for progress tracking
    cursor_src.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total_records = cursor_src.fetchone()[0]

    # Fetch records in batches
    last_seen_rowid = 0
    with tqdm(total=total_records,
              desc=f"Merging {db_file}", unit="records") as pbar:
        while True:
            cursor_src.execute(
                f"SELECT id, embedding, ROWID FROM {TABLE_NAME} WHERE ROWID > {last_seen_rowid} ORDER BY ROWID LIMIT {BATCH_SIZE}"
            )
            records = cursor_src.fetchall()
            if not records:
                break  # No more records to process

            # Prepare batch for insertion
            batch_data = []
            for id_, embedding, rowid in records:
                last_seen_rowid = rowid
                if embedding and embedding.strip():
                    if is_valid_json(embedding):
                        # Convert JSON string to list of floats
                        embedding = json.loads(embedding)

                        # Convert list of floats to Base64-encoded binary
                        binary_data = np.array(embedding, dtype=np.float32).tobytes()
                        embedding = base64.b64encode(binary_data).decode('utf-8')

                    if is_valid_base64(embedding):
                        batch_data.append((id_, embedding, embedding))

            # Perform batch insert/update
            if batch_data:
                cursor_merged.executemany(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, embedding)
                    VALUES (?, ?)
                    ON CONFLICT(id) DO UPDATE SET embedding = ?
                    """,
                    batch_data
                )
                conn_merged.commit()

            pbar.update(len(records))  # Update tqdm progress bar

    conn_src.close()

# Close merged database connection
conn_merged.close()
print(f"Merge completed! Combined database saved as {merged_db}")
