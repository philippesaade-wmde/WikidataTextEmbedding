#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

WDTEXTIFIER_DIR="$ROOT_DIR/WikidataTextifier"
WDTEXTIFIER_REPO="${WDTEXTIFIER_REPO:-https://github.com/wmde/WikidataTextifier.git}"
WDTEXTIFIER_REF="${WDTEXTIFIER_REF:-main}"
WDTEXTIFIER_PROJECT="wikidatatextifier"
WDTEXTIFIER_COMPOSE_FILE="$WDTEXTIFIER_DIR/docker-compose.yml"
PIPELINE_PROJECT="wikidatatextembedding-pipeline"
ENV_FILE="$ROOT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$WDTEXTIFIER_COMPOSE_FILE" ]]; then
  if [[ -d "$WDTEXTIFIER_DIR/.git" ]]; then
    echo "WikidataTextifier repo exists but compose file is missing: $WDTEXTIFIER_COMPOSE_FILE" >&2
    exit 1
  fi
  git clone "$WDTEXTIFIER_REPO" "$WDTEXTIFIER_DIR"
fi

git -C "$WDTEXTIFIER_DIR" fetch --prune --tags origin

if git -C "$WDTEXTIFIER_DIR" show-ref --verify --quiet "refs/remotes/origin/$WDTEXTIFIER_REF"; then
  git -C "$WDTEXTIFIER_DIR" checkout "$WDTEXTIFIER_REF"
  git -C "$WDTEXTIFIER_DIR" pull --ff-only origin "$WDTEXTIFIER_REF"
else
  git -C "$WDTEXTIFIER_DIR" checkout "$WDTEXTIFIER_REF"
fi

docker compose \
  -p "$WDTEXTIFIER_PROJECT" \
  -f "$WDTEXTIFIER_COMPOSE_FILE" \
  --env-file "$ENV_FILE" \
  up -d db wikibase wdtextifier

WD_CONTAINER_ID="$(
  docker compose \
    -p "$WDTEXTIFIER_PROJECT" \
    -f "$WDTEXTIFIER_COMPOSE_FILE" \
    --env-file "$ENV_FILE" \
    ps -q wdtextifier
)"

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

  if [[ "$HEALTH_STATUS" == "$TARGET_STATUS" ]]; then
    break
  fi
  sleep 2
done

if [[ "$HEALTH_STATUS" != "$TARGET_STATUS" ]]; then
  echo "wdtextifier did not become $TARGET_STATUS (status: $HEALTH_STATUS)." >&2
  exit 1
fi

cd "$ROOT_DIR"

# Ensure latest local code is used inside the pipeline container image.
docker compose \
  -p "$PIPELINE_PROJECT" \
  --env-file "$ENV_FILE" \
  build pipeline

WDTEXTIFIER_COMPOSE_NETWORK="${WDTEXTIFIER_PROJECT}_default" \
  docker compose \
    -p "$PIPELINE_PROJECT" \
    --env-file "$ENV_FILE" \
    run --rm pipeline "$@"
