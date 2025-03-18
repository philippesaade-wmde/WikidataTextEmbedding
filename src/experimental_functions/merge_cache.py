import sqlite3
import glob
import os
import base64
from tqdm import tqdm

# Define database file pattern
db_files = glob.glob("../data/Wikidata/sqlite_cacheembeddings_*.db")
db_files = ["../data/Wikidata/sqlite_cacheembeddings_4.db"]

# Define the target merged database
merged_db = "../data/Wikidata/sqlite_cacheembeddings.db"
TABLE_NAME = "wikidata_prototype"

# Batch size for processing
BATCH_SIZE = 1000  # Adjust based on performance needs

# Create the merged database connection
conn_merged = sqlite3.connect(merged_db)
cursor_merged = conn_merged.cursor()

# Create table in the merged database if it doesn't exist
cursor_merged.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id TEXT PRIMARY KEY,
    embedding TEXT
);
""")
conn_merged.commit()

# Helper function to check if a string is a valid Base64 encoding
def is_valid_base64(s):
    try:
        if not s or not isinstance(s, str):
            return False
        base64.b64decode(s, validate=True)
        return True
    except Exception:
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
    offset = 0
    with tqdm(total=total_records,
              desc=f"Merging {db_file}", unit="records") as pbar:
        while True:
            cursor_src.execute(f"SELECT id, embedding FROM {TABLE_NAME} LIMIT {BATCH_SIZE} OFFSET {offset}")
            records = cursor_src.fetchall()
            if not records:
                break  # No more records to process

            # Prepare batch for insertion
            batch_data = []
            for id_, embedding in records:
                if embedding and embedding.strip() and is_valid_base64(embedding):
                    cursor_merged.execute(f"SELECT embedding FROM {TABLE_NAME} WHERE id = ?", (id_,))
                    existing = cursor_merged.fetchone()

                    if existing is None or not is_valid_base64(existing[0]):
                        batch_data.append((id_, embedding, embedding))  # Prepare for bulk insert

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

            offset += BATCH_SIZE  # Move to the next batch
            pbar.update(len(records))  # Update tqdm progress bar

    conn_src.close()

# Close merged database connection
conn_merged.close()
print(f"Merge completed! Combined database saved as {merged_db}")
