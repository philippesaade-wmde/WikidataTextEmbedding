# Wikidata Embedding Project

## Overview
The Wikidata Embedding Project is an initiative led by Wikimedia Deutschland, in collaboration with [Jina.AI](https://jina.ai/) and [DataStax](https://www.datastax.com/). The project’s aim is to enhance the search functionality of Wikidata by integrating advanced vector-based semantic search. By employing advanced machine learning models and scalable vector databases, the project seeks to support the open-source community in developing innovative AI applications and use Wikidata's multilingual and inclusive knowledge graph, while making its extensive data more accessible, and contextually relevant for users across the globe.

For more details, visit [the Wikidata Embedding Project page](https://www.wikidata.org/wiki/Wikidata:Embedding_Project).

## Getting Started
This project contains several Docker containers to process the Wikidata Dump, save relevant data in a SQLite database, and then push the data to a DataStax vector database. Please run the containers sequentially as described below.

---

### Container: `data_processing_save_labels_descriptions`

#### Functionality
This container reads the Wikidata dump and extracts the labels and descriptions of all entities, storing them in an SQLite database. It also records whether each entity is linked to a Wikipedia page in the specified language. This forms the foundation for multilingual semantic search by ensuring that entities are both identifiable and contextually described.

#### Environment Variables
| Variable        | Default Value | Description |
|-----------------|--------------|-------------|
| `FILEPATH`      | `../data/Wikidata/latest-all.json.bz2` | Path to the Wikidata dump file |
| `PUSH_SIZE`    | `20000`        | 	Number of entities batched before being written to SQLite |
| `QUEUE_SIZE`    | `1500`        | Size of the queue buffering processed lines from the dump |
| `NUM_PROCESSES` | `4`           | Number of processes used to parse and extract entity metadata |
| `SKIPLINES`     | `0`           | Number of lines to skip in the dump file (useful for resuming) |
| `LANGUAGE`      | `'en'`        | Language filter used for Wikipedia linkage detection |

#### Output
- Creates an SQLite database (sqlite.db) with the following fields for each entity:
    - id: Wikidata entity ID (Q/P)
    - labels: All language-specific labels (JSON)
    - descriptions: All language-specific descriptions (JSON)
    - in_wikipedia: Boolean flag for whether the entity has a sitelink in the specified language
- This file is used as a filter to identify relevant entities for the next processing stage.

---

### Container: `data_processing_save_items_per_lang`

#### Functionality
This container reads the Wikidata dump and extracts complete entity information (labels, descriptions, aliases, claims) only for entities that are present in the specified language’s Wikipedia. The filtered entities are normalized and stored in a language-specific SQLite database for later processing.

#### Environment Variables
| Variable        | Default Value | Description |
|-----------------|--------------|-------------|
| `FILEPATH`      | `../data/Wikidata/latest-all.json.bz2` | Path to the Wikidata dump file |
| `PUSH_SIZE`    | `2000`        | Number of entities batched before being written to SQLite |
| `QUEUE_SIZE`    | `1500`        | Size of the multiprocessing queue buffering parsed lines |
| `NUM_PROCESSES` | `4`           | Number of parallel processes used to process entities |
| `SKIPLINES`     | `0`           | Number of lines to skip in the data dump (useful for resuming processing) |
| `LANGUAGE`      | `'en'`        | Language filter for checking Wikipedia inclusion and extracting language-specific metadata |
| `DB_PATH`      | `sqlite_enwiki.db`        | Path to the language-specific SQLite database where entity data is saved |

#### Output
- Generates a language-specific SQLite database (e.g., sqlite_enwiki.db) containing full entity records for items present in the target language's Wikipedia.
- Each entity includes:
    - id, label, description, aliases, claims, and other structured metadata
- This file is the primary data source for textification and embedding.

---

### Container: `add_wikidata_to_db`

#### Functionality
This container processes entities from a language-specific SQLite database, generates semantic text representations for each, embeds the text using the selected model (e.g., jina-embeddings-v3), and uploads the resulting vectors and metadata to DataStax Astra DB.

You can use either the full Wikidata dataset or a sample (by specifying a pickled file of QIDs). The entity text is constructed using a customizable formatting script in src/language_variables.

#### Environment Variables
| Variable              | Default Value | Description |
|----------------------|--------------|-------------|
| `MODEL`             | `jina`        | Embedding model name [`jina-embeddings-v3`](https://huggingface.co/jinaai/jina-embeddings-v3) |
| `SAMPLE`           | `false`        | If true, loads only a sample of QIDs from a pickle file |
| `SAMPLE_PATH`           | `../data/Evaluation Data/Sample IDs (EN).pkl`        | Path to the sample list of QIDs |
| `EMBED_BATCH_SIZE`  | `100`         | Number of vectors pushed to Astra per batch |
| `QUERY_BATCH_SIZE`  | `1000`        | Number of entities fetched from SQLite per iteration |
| `OFFSET`            | `0`           | Number of entities to skip before processing (useful for resume) |
| `API_KEY`  | `None`        | Filename of the DataStax API token, located in ../API_tokens |
| `COLLECTION_NAME`   | `None`        | Name of the Astra DB collection where vectors are stored |
| `LANGUAGE`          | `'en'`        | Language code for the source data |
| `TEXTIFIER_LANGUAGE`| `None`    | Script name from src/language_variables, defaults to LANGUAGE |
| `DUMPDATE`          | `09/18/2024`  | Metadata tag for the original Wikidata dump date |
| `DB_PATH`          | `sqlite_enwiki.db`  | Path to the SQLite file containing the entity data |

#### Output
- Embeds entities from the SQLite database and stores their vector representations in DataStax Astra DB under the specified COLLECTION_NAME.
- Each document includes:
    - text representation chunks
    - Metadata such as Label, Description, Aliases, Language, QID/PID, ChunkID, IsItem, IsProperty, DumpDate, and MD5 hash
- If SAMPLE=true, only entities from a predefined .pkl file are embedded and stored.

---

### Container: `run_retrieval`

#### Functionality
This container evaluates semantic retrieval by embedding a batch of user queries and retrieving the most similar Wikidata entities from DataStax Astra DB. It supports both standard retrieval and comparative evaluation (e.g. to measure how close "correct" vs. "incorrect" QIDs are to the query).

Each query is embedded using the same model used during indexing (e.g., jina-embeddings-v3) and matched against the stored collection of Wikidata entity vectors. The results, including QIDs and similarity scores, are saved to a pickled evaluation file.

#### Environment Variables
| Variable            | Default Value | Description |
|--------------------|--------------|-------------|
| `MODEL`           | `jina`        | Embedding model name [`jina-embeddings-v3`](https://huggingface.co/jinaai/jina-embeddings-v3) |
| `BATCH_SIZE`      | `100`         | 	Number of queries processed in each evaluation batch |
| `API_KEY`| `None`        | Filename of the DataStax API key from ../API_tokens/ |
| `COLLECTION_NAME` | `None`        | Name of the vector database collection (required) |
| `EVALUATION_PATH` | `None`        | Path to the pickled evaluation dataset (e.g., Mintaka) |
| `QUERY_COL`       | `None`        | Column name in the evaluation file containing the queries |
| `QUERY_LANGUAGE`  | `'en'`        | Language of the input queries |
| `DB_LANGUAGE`     | `None`        | Filter for entities in this language (comma-separated list)|
| `K`               | `50`          | Number of top results retrieved per query |
| `RESTART`         | `false`       | If true, restarts evaluation from scratch and overwrites previous results |
| `COMPARATIVE`     | `false`       | If true, performs distance comparison using QIDs from specific columns |
| `COMPARATIVE_COLS`| `None`        | Comma-separated names of DataFrame columns containing QIDs to compare |
| `PREFIX`          | `''`          | Optional string appended to the output filename |
| `KEYWORDSEARCH`          | `''`          | Evaluate keyword-based retrieval instead of vector search |

#### Output
- The results are saved in a file named like:
    `retrieval_results_<EVALUATION_SET>-<COLLECTION_NAME>-DB(<DB_LANGUAGE>)-Query(<QUERY_LANGUAGE>)<PREFIX>.pkl`
- Saved file includes:
    - `Retrieval QIDs`: list of top-K retrieved entity IDs
    - `Retrieval Score`: corresponding similarity scores
- Progress is automatically saved every 100 queries.

---

### Container: `run_rerank`

#### Functionality
This container reranks the list of QIDs retrieved from the vector database by scoring their textual representations against the original query using a reranker model (JinaAIReranker). Each candidate entity is reconstructed from the SQLite database and converted to plain text using the WikidataTextifier.

The reranker assigns a relevance score to each query-entity pair, reorders the list accordingly, and saves the result back to the evaluation file used during retrieval. This step refines the output by focusing on contextual alignment between the query and the entity text.

#### Environment Variables
| Variable            | Default Value | Description |
|--------------------|--------------|-------------|
| `MODEL`           | `jina`        | Reranker model name [`jina-reranker-v2-base-multilingual`](https://huggingface.co/jinaai/jina-reranker-v2-base-multilingual) |
| `BATCH_SIZE`      | `100`         | 	Number of queries reranked per batch |
| `QUERY_COL`       | `None`        | Column name in the evaluation file containing the queries |
| `RETRIEVAL_FILENAME`  | `'en'`        | Filename (without extension) of the .pkl file produced by the run_retrieval container |
| `LANGUAGE`          | `'en'`        | Language code for the source data |
| `TEXTIFIER_LANGUAGE`| `None`    | Script name from src/language_variables, defaults to LANGUAGE |
| `RESTART`         | `false`       | If true, restarts evaluation from scratch and overwrites previous results |

#### Output
- Adds a new column Reranked QIDs to the evaluation DataFrame.
- The updated data is saved back to ../data/Evaluation Data/{RETRIEVAL_FILENAME}.pkl.
- Progress is automatically saved every 100 queries.

---

### Container: `push_huggingface`

#### Functionality
This container uploads processed Wikidata entity chunks to the Hugging Face Hub as dataset splits. Each chunk is loaded from a compressed JSON file, cleaned, and transformed into a datasets.Dataset object using a parallelized generator. The upload process is resumable and only processes chunks that haven't already been pushed.

Each entity contains structured data including labels, descriptions, aliases, sitelinks, and claims (including all missing labels in the claims).

#### Environment Variables
| Variable            | Default Value | Description |
|--------------------|--------------|-------------|
| `QUEUE_SIZE`           | `5000`        | Size of the multiprocessing queue used for ingestion |
| `NUM_PROCESSES`      | `4`         | 	Number of worker processes parsing each chunk |
| `SKIPLINES`       | `0`        | Number of lines to skip from the top of the dump (for resume) |
| `API_KEY`  | `huggingface_api.json`        | Filename containing the Hugging Face API token (must define API_KEY key inside the file) |
| `ITERATION`          | `0`        | 	Index of the current chunk being processed (only used for logging) |

#### Output
- Publishes entity chunks to the Hugging Face Hub under the dataset repo philippesaade/wikidata.
- Each file (e.g., chunk_0.json.gz) becomes a separate dataset split named chunk_0, chunk_1, etc.
- Each dataset row includes id, labels, descriptions, aliases, sitelinks, and claims.

---

### Container: `create_prototype`

#### Functionality
This container builds the vector database prototype directly from the Hugging Face-hosted Wikidata dataset, skipping the need to reprocess the original Wikidata dump locally. Each entity is streamed from the Hugging Face dataset, converted into a text representation using WikidataTextifier, and embedded using the selected model. The resulting vectors and metadata are stored in DataStax Astra DB.

This is the recommended container to run if you're starting from the public dataset and want to populate a new vector database collection without executing the earlier preprocessing containers.

Note: If the Hugging Face dataset is already available, this is the only container you need to create the vector database. You can skip the preprocessing containers (data_processing_save_labels_descriptions, data_processing_save_items_per_lang, and add_wikidata_to_db).

#### Environment Variables
| Variable            | Default Value | Description |
|----------------------|--------------|-------------|
| `MODEL`             | `jina`        | Embedding model name [`jina-embeddings-v3`](https://huggingface.co/jinaai/jina-embeddings-v3) |
| `NUM_PROCESSES`           | `4`        | 	Number of parallel processes used for embedding and upload |
| `EMBED_BATCH_SIZE`  | `100`         | Number of vectors pushed to Astra per batch |
| `QUEUE_SIZE`  | `2 * EMBED_BATCH_SIZE * NUM_PROCESSES`        | Size of multiprocessing queue for entity processing |
| `DB_API_KEY`  | `None`        | Filename of the DataStax API token, located in ../API_tokens |
| `COLLECTION_NAME`   | `None`        | Name of the Astra DB collection where vectors are stored |
| `LANGUAGE`          | `'en'`        | Language code for the source data |
| `TEXTIFIER_LANGUAGE`| `None`    | Script name from src/language_variables, defaults to LANGUAGE |
| `DUMPDATE`          | `09/18/2024`  | Metadata tag for the original Wikidata dump date |
| `CHUNK_SIZES_PATH`          | `Wikidata/wikidata_chunk_sizes_2024-09-18.json`  | Path to the JSON file storing number of entities per chunk |
| `CHUNK_NUM`          | `None`  | Chunk number to process (required for each container run) |
| `CHECK_IDS_PUSHED`          | `false`  | Chunk if items exist in the database before calculating embeddings |

#### Output
- Streams and embeds entities from philippesaade/wikidata on the Hugging Face Hub.
- Pushes chunks of embedded entities into the specified COLLECTION_NAME in Astra DB.
- Each entity is stored with full metadata, including label, description, aliases, instance of, and dump date.

---