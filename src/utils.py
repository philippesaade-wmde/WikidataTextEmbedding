
from copy import copy
import os
import socket
from datetime import datetime, timezone

def check_wdtextifier_stack():
    WIKIBASE_HOST = os.environ.get("WIKIBASE_HOST", "wikibase")
    WIKIBASE_PORT = int(os.environ.get("WIKIBASE_PORT", 80))
    TEXTIFIER_DB_HOST = os.environ.get("TEXTIFIER_DB_HOST", "db")
    TEXTIFIER_DB_PORT = int(os.environ.get("TEXTIFIER_DB_PORT", os.environ.get("DB_PORT", 3306)))

    try:
        with socket.create_connection(
            (WIKIBASE_HOST, WIKIBASE_PORT),
            timeout=3
        ):
            pass
    except OSError as e:
        raise RuntimeError("Cannot reach WD Textifier's Wikibase.") from e

    try:
        with socket.create_connection(
            (TEXTIFIER_DB_HOST, TEXTIFIER_DB_PORT),
            timeout=3
        ):
            pass
    except OSError as e:
        raise RuntimeError("Cannot reach WD Textifier's Label Database.") from e


def normalize_datetime(value):
    """Normalize timestamps to naive UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt = datetime.now(timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt


def _to_text_with_claims(entity, claims, lang):
    entity_view = copy(entity)
    entity_view.claims = claims
    return entity_view.to_text(lang=lang)


def _tokenize_with_offsets(text, tokenizer):
    tokens = tokenizer(
        text,
        add_special_tokens=False,
        return_offsets_mapping=True,
    )

    if "input_ids" not in tokens or "offset_mapping" not in tokens:
        raise ValueError("Tokenizer must return 'input_ids' and 'offset_mapping'.")

    return tokens["input_ids"], tokens["offset_mapping"]


def _trim_to_length(text, offsets, max_length):
    if not offsets:
        return text
    if len(offsets) < max_length:
        return text[: offsets[-1][1]]
    return text[: offsets[max_length - 1][1]]


def chunk_item_text(item, tokenizer, max_length=1024, lang="en", sticky_property_ids=("P31",)):
    """
    Chunk text using the same strategy as src/wikidataEmbed.py::chunk_text.
    """
    entity_text = item.to_text(lang=lang)
    token_ids, offsets = _tokenize_with_offsets(entity_text, tokenizer)

    # If full text already fits, return one chunk.
    if len(token_ids) < max_length:
        return [entity_text]

    sticky_claims = [c for c in item.claims if c.property.id in sticky_property_ids]
    claims = [c for c in item.claims if c.property.id not in sticky_property_ids]

    # If label/description (+sticky claims) already exceed max, return trimmed chunk.
    entity_description = _to_text_with_claims(item, sticky_claims, lang=lang)
    token_ids, offsets = _tokenize_with_offsets(entity_description, tokenizer)
    if len(token_ids) >= max_length:
        return [_trim_to_length(entity_description, offsets, max_length)]

    if not claims:
        return [entity_description]

    chunks = []
    start = 0
    total_claims = len(claims)

    while start < total_claims:
        lo = start + 1
        hi = total_claims + 1
        best_end = None
        best_text = None
        best_offsets = None

        # Find the largest claim span that still fits.
        while lo < hi:
            mid = (lo + hi) // 2
            entity_text = _to_text_with_claims(
                item,
                [*sticky_claims, *claims[start:mid]],
                lang=lang,
            )
            token_ids, offsets = _tokenize_with_offsets(entity_text, tokenizer)

            if len(token_ids) < max_length:
                best_end = mid
                best_text = entity_text
                best_offsets = offsets
                lo = mid + 1
            else:
                hi = mid

        # Single claim (plus sticky context) is too long, so emit trimmed chunk.
        if best_end is None:
            entity_text = _to_text_with_claims(
                item,
                [*sticky_claims, claims[start]],
                lang=lang,
            )
            token_ids, offsets = _tokenize_with_offsets(entity_text, tokenizer)
            chunks.append(_trim_to_length(entity_text, offsets, max_length))
            start += 1
            continue

        chunks.append(_trim_to_length(best_text, best_offsets, max_length))
        start = best_end

    return chunks

def extract_instanceof(item, instanceof_pid="P31"):
    instanceof = [claim.values for claim in item.claims if claim.property.id == instanceof_pid]
    if not instanceof:
        return []

    return [value.value.id for value in instanceof[0] if hasattr(value.value, "id")]

def extract_pids(item):
    return [claim.property.id for claim in item.claims]
