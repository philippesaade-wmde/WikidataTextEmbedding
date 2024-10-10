"""python
pip install -U sentence-transformers
pip install requests  # google-search-results beautifulsoup4
# !pip install langchain_community langchain
"""

from multiprocessing import Pool, cpu_count
from sentence_transformers import SentenceTransformer
from time import time

from wikidata_textification import WikidataTextification

lang = 'en'
timeout = 10
n_cores = cpu_count()
verbose = False
wikidata_base = '"wikidata.org"'
return_list = True
version = 1
n_qids = 100  # number of QIDs to embed

# List of the first `n_qids` QIDs
qids = [f'Q{k+1}' for k in range(n_qids)]

if 'embedder' not in locals():
    embedder = SentenceTransformer(
        "jinaai/jina-embeddings-v2-base-en",
        trust_remote_code=True
    )

logger = WikidataTextification.get_logger(__name__)

timeout = 1
n_qids = 1000  # number of QIDs to embed
qids = [f'Q{k+1}' for k in range(n_qids)]
save_filename = f'wikidata_vectordb_first_{n_qids}_{lang}.csv'

wd_textification = WikidataTextification(
    embedder=embedder,
    lang=lang,
    timeout=timeout,
    n_cores=n_cores,
    version=version,
    verbose=verbose,
    wikidata_base=wikidata_base,
    return_list=return_list,
    save_filename=save_filename
)

print(qids)

start = time()
wd_statements = wd_textification.get_wikidata_statements(qids)
print(time() - start)

print(len(wd_textification.wikidata_statements))

# df_vecdb = pd.DataFrame(wd_textification.wikidata_statements)

# df_vecdb.to_csv(f'wikidata_vectordb_first_{n_qids}_{lang}.csv')

print(wd_textification.df_vecdb)
