import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.wikidataVectorDB import AstraDBConnect


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare AstraDB document IDs with the local SQLite vector cache."
    )
    parser.add_argument("--lang", required=True)
    parser.add_argument(
        "--entity-type",
        choices=("items", "items_nositelinks", "properties"),
        default="items",
    )
    parser.add_argument("--data-dir", default="data/Wikidata")
    parser.add_argument("--config", default="API_tokens/datastax_api.json")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--print-mismatches", action="store_true")
    return parser.parse_args()


def find_local_ids(connection, ids):
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"SELECT id FROM vectors WHERE id IN ({placeholders})",
        ids,
    )
    return {row[0] for row in rows}


def main():
    args = parse_args()
    database_path = (
        Path(args.data_dir)
        / f"sqlite_wikidata_vectors_{args.entity_type}_{args.lang}.db"
    )
    if not database_path.is_file():
        raise SystemExit(f"Local database not found: {database_path}")

    astra = AstraDBConnect(lang=args.lang, entity_type=args.entity_type, config_path=args.config)
    collection = astra.collection

    with sqlite3.connect(f"file:{database_path.resolve()}?mode=ro", uri=True) as local:
        local_count = local.execute("SELECT COUNT(*) FROM vectors").fetchone()[0]
        estimated_astra_count = collection.estimated_document_count()
        print(f"Local SQLite count:       {local_count:,}")
        print(f"Astra estimated count:    {estimated_astra_count:,}")
        print("Scanning Astra IDs for an exact comparison...")

        scanned_count = 0
        astra_only_count = 0
        astra_only_samples = []
        batch = []

        cursor = collection.find({}, projection={"_id": True})
        for document in cursor:
            batch.append(document["_id"])
            if len(batch) < args.batch_size:
                continue

            local_ids = find_local_ids(local, batch)
            missing_ids = [document_id for document_id in batch if document_id not in local_ids]
            if args.print_mismatches:
                for document_id in missing_ids:
                    print(f"Astra-only ID: {document_id}", flush=True)
            astra_only_count += len(missing_ids)
            astra_only_samples.extend(
                missing_ids[: max(0, args.sample_limit - len(astra_only_samples))]
            )
            scanned_count += len(batch)
            batch.clear()

            if args.progress_every and scanned_count % args.progress_every < args.batch_size:
                print(
                    f"Scanned {scanned_count:,}; "
                    f"Astra-only documents found: {astra_only_count:,}"
                )

        if batch:
            local_ids = find_local_ids(local, batch)
            missing_ids = [document_id for document_id in batch if document_id not in local_ids]
            if args.print_mismatches:
                for document_id in missing_ids:
                    print(f"Astra-only ID: {document_id}", flush=True)
            astra_only_count += len(missing_ids)
            astra_only_samples.extend(
                missing_ids[: max(0, args.sample_limit - len(astra_only_samples))]
            )
            scanned_count += len(batch)

    print(f"Astra exact scanned count: {scanned_count:,}")
    print(f"Astra-only documents:      {astra_only_count:,}")
    print(f"Count difference:          {scanned_count - local_count:+,}")
    if astra_only_samples:
        print("Sample Astra-only IDs:")
        for document_id in astra_only_samples:
            print(f"  {document_id}")


if __name__ == "__main__":
    main()
