# Wikidata Text Embedding Pipeline

This repository processes a Wikidata dump with WikidataTextifier, produces text chunks, embeds them with Jina, pushes vectors to Astra DB, and can publish both cleaned Wikidata JSON and cached vectors to Hugging Face datasets.

## Current Pipeline

`main.py` is the entrypoint and supports:

1. Download dump (if missing or `FORCE_DOWNLOAD_DUMP=true`)
2. First pass (optional): save labels for all entities to Textifier MariaDB (`SAVE_LABELS=true`)
3. Second pass (optional): process entities for:
   - cleaned Wikidata JSON -> Hugging Face (`SAVE_WD_TO_HF=true`)
   - vectorization -> Astra DB + local vector cache (`SAVE_TO_VECTORDB=true`)
4. Optional post-step: publish local cached vectors to Hugging Face (`SAVE_VECTORS_TO_HF=true`)
5. Optional multi-language loop with `WD_LANGS`

When `WD_LANGS` is set and `SAVE_LABELS=true`, labels are saved once, then second-pass processing runs once per language.

## Filtering and Processing Rules

### Item (`Q*`) filter for vectorization

- has label in `WD_LANG`, `FALLBACK_LANG`, or `mul`
- has claims or description
- is not disambiguation (`P31 != Q4167410`)
- has at least one sitelink ending in `wiki`

### Property (`P*`) filter for vectorization

- has label in `WD_LANG`, `FALLBACK_LANG`, or `mul`
- has claims or description

### Claim cleanup before textification

- drop values of datatype: `external-id`, `commonsMedia`, `url`, `geo-shape`, `tabular-data`
- keep one value for `monolingualtext`
- for properties (`P*`), drop claim-properties in `PROPERTY_CONSTRAINT_PIDS` (default: `P2302`)

### Chunking and metadata

- Text is chunked to max tokenizer length (`1024` tokens)
- Metadata stored with each vector chunk:
  - `QID` or `PID`
  - `ChunkID`
  - `Language`
  - `Label`
  - `Description`
  - `InstanceOf`
  - `Properties`
  - `LastModified`
  - `DumpDate`

## Credentials and Token Files

By default, `main.py` reads token/config files from `API_tokens/`:

- `API_tokens/jina_api.json`
  - `{"API_KEY": "..."}`
- `API_tokens/datastax_api.json`
  - `{"ASTRA_DB_APPLICATION_TOKEN": "...", "ASTRA_DB_API_ENDPOINT": "...", "COLLECTION_PREFIX": "..."}`
- `API_tokens/wd_hf_api.json`
  - `{"REPO_ID": "...", "API_KEY": "..."}`
- `API_tokens/vectors_hf_api.json`
  - `{"REPO_ID": "...", "API_KEY": "..."}`

## Environment Variables

### Core toggles

| Variable | Default | Description |
|---|---|---|
| `SAVE_LABELS` | `false` | Run first pass: save all entity labels to Textifier DB |
| `SAVE_WD_TO_HF` | `false` | Run second pass: publish cleaned Wikidata JSON to HF |
| `SAVE_TO_VECTORDB` | `false` | Run second pass: embed and push to Astra DB + local vector cache |
| `SAVE_VECTORS_TO_HF` | `false` | Publish local cached vectors to HF |
| `FORCE_DOWNLOAD_DUMP` | `false` | Force re-download of dump |

### Language

| Variable | Default | Description |
|---|---|---|
| `WD_LANG` | `en` | Active language for single-language run |
| `FALLBACK_LANG` | `WD_LANG` | Fallback language |
| `WD_LANGS` | empty | Comma-separated languages for per-language second pass loop |
| `FALLBACK_LANG_<LANG>` | unset | Per-language fallback override, example: `FALLBACK_LANG_DE=en` |

### Dump and performance tuning

| Variable | Default | Description |
|---|---|---|
| `DUMP_PATH` | `data/wd_dump.gz` | Dump file path |
| `READER_QUEUE_SIZE` | `128` | Reader queue size (in batches) |
| `READER_BATCH_SIZE` | `16` | Lines per queue batch |
| `HF_CHUNK_SIZE` | `10000` | Rows per HF upload chunk |
| `DUMP_DATE` | current UTC date | Metadata dump date |
| `PROPERTY_CONSTRAINT_PIDS` | `P2302` | Comma-separated claim-property IDs to drop for `P*` textification |

### HF publishing

| Variable | Default | Description |
|---|---|---|
| `HF_BRANCH` | UTC timestamp | Branch for cleaned WD dataset uploads |
| `VECTOR_HF_BRANCH` | `HF_BRANCH` | Branch for vector dataset uploads |
| `WD_HF_API_PATH` | `./API_tokens/wd_hf_api.json` | WD dataset token/repo config |
| `VECTORS_HF_API_PATH` | `./API_tokens/vectors_hf_api.json` | Vector dataset token/repo config |

When `WD_LANGS` is set, branch names are suffixed per language (`<branch>-en`, `<branch>-de`, ...).

### External service connectivity

| Variable | Default | Description |
|---|---|---|
| `WIKIBASE_HOST` | `wikibase` | Host checked before dump processing |
| `WIKIBASE_PORT` | `80` | Wikibase port |
| `TEXTIFIER_DB_HOST` | `db` | Label DB host checked before dump processing |
| `TEXTIFIER_DB_PORT` | `3306` | Label DB port |
| `DB_HOST` | `db` | Textifier DB host used by Textifier internals |
| `DB_PORT` | `3306` | Textifier DB port |
| `DB_NAME` | none | Textifier label DB name |
| `DB_USER` | none | Textifier DB user |
| `DB_PASS` | none | Textifier DB password |

### Runner script variables

| Variable | Default | Description |
|---|---|---|
| `WDTEXTIFIER_REPO` | `https://github.com/wmde/WikidataTextifier.git` | WikidataTextifier Git repo to clone/update |
| `WDTEXTIFIER_REF` | `main` | Branch/tag/commit checked out in `WikidataTextifier` |

## Local Run

Install dependencies:

```bash
uv sync --locked
```

Example: labels pass only

```bash
SAVE_LABELS=true \
SAVE_WD_TO_HF=false \
SAVE_TO_VECTORDB=false \
SAVE_VECTORS_TO_HF=false \
uv run python main.py
```

Example: second pass to Astra only

```bash
SAVE_LABELS=false \
SAVE_WD_TO_HF=false \
SAVE_TO_VECTORDB=true \
SAVE_VECTORS_TO_HF=false \
WD_LANG=en \
FALLBACK_LANG=en \
uv run python main.py
```

Example: publish cached vectors to HF only

```bash
SAVE_LABELS=false \
SAVE_WD_TO_HF=false \
SAVE_TO_VECTORDB=false \
SAVE_VECTORS_TO_HF=true \
WD_LANG=en \
uv run python main.py
```

## Docker Run

`docker-compose.yml` in this repository is **pipeline-only**.
It does not start `db` / `wikibase` / `wdtextifier` by itself.

Use the helper script to run everything end-to-end:

1. clone/update `WikidataTextifier`
2. checkout configured ref (`WDTEXTIFIER_REF`)
3. start Textifier stack (`db`, `wikibase`, `wdtextifier`)
4. wait for `wdtextifier` health
5. run this repo's `pipeline` container on the same Docker network

```bash
./scripts/run_pipeline_with_wdtextifier.sh
```

### Manual (Without Script)

If you want to run manually:

1. Start Textifier stack:

```bash
docker compose \
  -p wikidatatextifier \
  -f /path/to/WikidataTextEmbedding/WikidataTextifier/docker-compose.yml \
  --env-file /path/to/WikidataTextEmbedding/.env \
  up -d db wikibase wdtextifier
```

2. Run pipeline container:

```bash
WDTEXTIFIER_COMPOSE_NETWORK=wikidatatextifier_default \
docker compose \
  -p wikidatatextembedding-pipeline \
  -f /path/to/WikidataTextEmbedding/docker-compose.yml \
  --env-file /path/to/WikidataTextEmbedding/.env \
  run --rm pipeline
```

## Output Artifacts

- Local vector cache SQLite files:
  - `data/Wikidata/sqlite_wikidata_vectors_items_<lang>.db`
  - `data/Wikidata/sqlite_wikidata_vectors_properties_<lang>.db`
- Hugging Face dataset uploads:
  - cleaned Wikidata rows under `data/` (branch `HF_BRANCH`)
  - vectors under `data/<lang>/` (branch `VECTOR_HF_BRANCH`)

## Notes

- If all `SAVE_*` flags are `false`, the run exits without processing.
- `main.py` checks network reachability to Wikibase and label DB before processing dump passes.
- Hugging Face uploads run in a background uploader process and use temporary cache dirs that are cleaned after each chunk.
