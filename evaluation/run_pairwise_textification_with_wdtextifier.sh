#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WDTEXTIFIER_DIR="$ROOT_DIR/WikidataTextifier"
WDTEXTIFIER_PROJECT="wikidatatextifier"
WDTEXTIFIER_COMPOSE_FILE="$WDTEXTIFIER_DIR/docker-compose.yml"
PIPELINE_PROJECT="wikidatatextembedding-pipeline"
ENV_FILE="$ROOT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$WDTEXTIFIER_COMPOSE_FILE" ]]; then
  echo "Local WikidataTextifier checkout is missing: $WDTEXTIFIER_COMPOSE_FILE" >&2
  exit 1
fi

docker compose \
  -p "$WDTEXTIFIER_PROJECT" \
  -f "$WDTEXTIFIER_COMPOSE_FILE" \
  --env-file "$ENV_FILE" \
  up -d db wikibase wdtextifier

WD_CONTAINER_ID="$(docker compose \
  -p "$WDTEXTIFIER_PROJECT" \
  -f "$WDTEXTIFIER_COMPOSE_FILE" \
  --env-file "$ENV_FILE" \
  ps -q wdtextifier)"

if [[ -z "$WD_CONTAINER_ID" ]]; then
  echo "Could not find wdtextifier container ID after startup." >&2
  exit 1
fi

HAS_HEALTHCHECK="$(docker inspect -f '{{if (index .State "Health")}}yes{{else}}no{{end}}' "$WD_CONTAINER_ID")"
TARGET_STATUS="running"
if [[ "$HAS_HEALTHCHECK" == "yes" ]]; then
  TARGET_STATUS="healthy"
fi

for _ in $(seq 1 120); do
  if [[ "$HAS_HEALTHCHECK" == "yes" ]]; then
    HEALTH_STATUS="$(docker inspect -f '{{(index .State "Health").Status}}' "$WD_CONTAINER_ID")"
  else
    HEALTH_STATUS="$(docker inspect -f '{{.State.Status}}' "$WD_CONTAINER_ID")"
  fi
  [[ "$HEALTH_STATUS" == "$TARGET_STATUS" ]] && break
  sleep 2
done

if [[ "$HEALTH_STATUS" != "$TARGET_STATUS" ]]; then
  echo "wdtextifier did not become $TARGET_STATUS (status: $HEALTH_STATUS)." >&2
  exit 1
fi

cd "$ROOT_DIR"
docker compose \
  -p "$PIPELINE_PROJECT" \
  --env-file "$ENV_FILE" \
  build pipeline

INPUT_CSV_HOST="${PAIRWISE_INPUT_CSV:-$ROOT_DIR/evaluation/pairwise_evaluation_dataset.csv}"
OUTPUT_DIR_HOST="${PAIRWISE_OUTPUT_DIR:-$ROOT_DIR/evaluation/textified}"
LANGUAGES="${PAIRWISE_LANGUAGES:-${WD_LANGS:-}}"
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
PIPELINE_ARGS+=("$@")

WDTEXTIFIER_COMPOSE_NETWORK="${WDTEXTIFIER_PROJECT}_default" \
  docker compose \
    -p "$PIPELINE_PROJECT" \
    --env-file "$ENV_FILE" \
    run --rm \
    -v "$ROOT_DIR/evaluation:/workspace/evaluation" \
    pipeline "${PIPELINE_ARGS[@]}"
