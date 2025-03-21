import sqlite3
import base64
from tqdm import tqdm
import json

# Define the target merged database
db_path = "../../data/Wikidata/wikidata_cache.db"
TABLE_NAME = "wikidata_prototype"

# Batch size for processing
BATCH_SIZE = 1000  # Adjust based on performance needs

# Create the merged database connection
conn = sqlite3.connect(db_path)
cursor= conn.cursor()

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

# Get total record count for progress tracking
cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
total_records = cursor.fetchone()[0]

# Fetch records in batches
base64_count = 0
json_count = 0
nonempty_count = 0
offset = 0
with tqdm(total=total_records, unit="records") as pbar:
    while True:
        cursor.execute(f"SELECT id, embedding FROM {TABLE_NAME} LIMIT {BATCH_SIZE} OFFSET {offset}")
        records = cursor.fetchall()
        if not records:
            break  # No more records to process

        for id_, embedding in records:
            if embedding and embedding.strip():
                if is_valid_base64(embedding):
                    base64_count += 1
                elif is_valid_json(embedding):
                    json_count += 1
                else:
                    nonempty_count += 1

        offset += BATCH_SIZE  # Move to the next batch
        pbar.update(len(records))  # Update tqdm progress bar

conn.close()
print(f"Base64 Count: {base64_count}, JSON Count: {json_count}, Non-empty Count: {nonempty_count}, Empty Count: {total_records - base64_count - json_count - nonempty_count}")
