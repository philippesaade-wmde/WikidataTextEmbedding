import sys
import pandas as pd

from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

max_string_len = 0
max_string_bytes = 0


def return_max(chunk):
    max_len = max(len(l_.item_str) for _, l_ in chunk.iterrows())
    max_mem = max(sys.getsizeof(l_.item_str) for _, l_ in chunk.iterrows())
    return max_len, max_mem


def max_max_pool(filename):
    filename = 'csvfiles/wikidata_vectordb_datadump_item_chunks_1000000_en.csv'
    with Pool(cpu_count()) as pool:
        # Wrap pool.imap with tqdm for progress tracking
        pool_imap = pool.imap(
            return_max,
            pd.read_csv(filename, chunksize=1000)
        )
        max_max = list(tqdm(pool_imap))


def max_max_load(filename, embed_chunksize=256):
    filename = 'csvfiles/wikidata_vectordb_datadump_item_chunks_1000000_en.csv'
    df = pd.read_csv(filename)
    max_string_len = 0
    max_string_bytes = 0

    for _, line_ in tqdm(df.iterrows()):
        max_string_len = max(max_string_len, len(line_.item_str))
        max_string_bytes = max(max_string_bytes, sys.getsizeof(line_.item_str))
    print(max_string_len)
    print(max_string_bytes)
    print(max_string_bytes/max_string_len)
    print(embed_chunksize / np.ceil(max_string_bytes))
