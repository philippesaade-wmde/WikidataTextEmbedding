#!/usr/bin/env bash
set -euo pipefail

# Run the batch evaluator inside the same local WikidataTextifier stack used
# by main.py.  The existing wrapper starts the services, builds the pipeline
# image, and joins its container to the wdtextifier network.  It now mounts the
# evaluation directory so this script and its output CSVs are visible there.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

INPUT_CSV_HOST="${PAIRWISE_INPUT_CSV:-$ROOT_DIR/evaluation/pairwise_evaluation_dataset.csv}"
OUTPUT_DIR_HOST="${PAIRWISE_OUTPUT_DIR:-$ROOT_DIR/evaluation/textified}"
LANGUAGES="${PAIRWISE_LANGUAGES:-${WD_LANGS:-}}"

if [[ "$INPUT_CSV_HOST" != "$ROOT_DIR/evaluation/"* ]]; then
  echo "PAIRWISE_INPUT_CSV must be inside $ROOT_DIR/evaluation so it is available in the pipeline container." >&2
  exit 1
fi
if [[ "$OUTPUT_DIR_HOST" != "$ROOT_DIR/evaluation/"* ]]; then
  echo "PAIRWISE_OUTPUT_DIR must be inside $ROOT_DIR/evaluation so it is available in the pipeline container." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR_HOST"

INPUT_CSV_CONTAINER="/workspace/evaluation/${INPUT_CSV_HOST#"$ROOT_DIR/evaluation/"}"
OUTPUT_DIR_CONTAINER="/workspace/evaluation/${OUTPUT_DIR_HOST#"$ROOT_DIR/evaluation/"}"

PIPELINE_ARGS=(
  /workspace/.venv/bin/python
  /workspace/evaluation/textify_pairwise_dataset.py
  --input-csv "$INPUT_CSV_CONTAINER"
  --output-dir "$OUTPUT_DIR_CONTAINER"
)
if [[ -n "$LANGUAGES" ]]; then
  PIPELINE_ARGS+=(--languages "$LANGUAGES")
fi

COMPOSE_FILE="$ROOT_DIR/docker-compose.yml:$ROOT_DIR/evaluation/docker-compose.evaluation.yml" \
  exec "$ROOT_DIR/scripts/run_pipeline_with_wdtextifier.sh" "${PIPELINE_ARGS[@]}" "$@"
