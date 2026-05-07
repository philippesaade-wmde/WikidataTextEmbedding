import orjson
import json
import os
import traceback
import tempfile
from time import sleep
from multiprocessing import Process, Queue, Value, Lock
from queue import Full
from huggingface_hub import HfApi, CommitOperationDelete
import pyarrow as pa
import pyarrow.parquet as pq


class WikidataHFDatasetPublisher:
    """
    Publish JSON-like records to a Hugging Face dataset repo in chunked parquet files.

    Designed for use inside `WikidataDumpReader.run(..., handler_receives_batch=True)`,
    while keeping local storage usage bounded.
    """

    def __init__(
        self,
        branch: str,
        config_path: str = None,
        storage_chunk_size: int = 1000,
        memory_chunk_size: int = 20,
        queue_size: int = 128,
        data_dir: str | None = 'data',
    ):
        self.storage_chunk_size = max(1, int(storage_chunk_size))
        self.memory_chunk_size = max(1, int(memory_chunk_size))
        self.closed = Value('i', 0)
        self.write_lock = Lock()
        self.queue = Queue(maxsize=max(1, int(queue_size)))
        self.branch = branch
        self.data_dir = data_dir

        self.token = os.environ.get("HF_TOKEN")
        self.repo_id = os.environ.get("HF_REPO_ID")
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f_in:
                data = json.load(f_in)
                self.token = data.get("API_KEY", self.token)
                self.repo_id = data.get("REPO_ID", self.repo_id)

        if not self.token:
            raise ValueError("Hugging Face API token not found.")
        if not self.repo_id:
            raise ValueError("Hugging Face repository ID not found.")

        self.chunk_idx = 0
        self.create_new_branch()

        self.uploader = Process(
            target=self._publish_records,
        )
        self.uploader.start()

    def create_new_branch(self):
        api = HfApi(token=self.token)

        refs = api.list_repo_refs(repo_id=self.repo_id, repo_type="dataset")
        for b in refs.branches:
            if b.name == self.branch:
                return False

        api.create_branch(repo_id=self.repo_id, branch=self.branch, repo_type="dataset")

        files = api.list_repo_files(repo_id=self.repo_id, repo_type="dataset", revision=self.branch)

        ops = [CommitOperationDelete(path_in_repo=f) for f in files if f.startswith("data/")]

        if ops:
            api.create_commit(
                repo_id=self.repo_id,
                repo_type="dataset",
                revision=self.branch,
                operations=ops,
                commit_message="Remove data directory from branch",
            )

        return True

    def _publish_records(self):
        api = HfApi(token=self.token)
        while True:
            chunk_fd, chunk_path = tempfile.mkstemp(prefix="hf_chunk_", suffix=".parquet")
            os.close(chunk_fd)

            num_records = 0
            reached_end = False
            try:
                num_records, reached_end = self._write_chunk_parquet(chunk_path)

                if num_records == 0:
                    if reached_end:
                        break
                    continue

                remote_base = (self.data_dir or "data").rstrip("/")
                remote_file = f"{remote_base}/chunk_{self.chunk_idx}.parquet"
                while True:
                    try:
                        api.upload_file(
                            path_or_fileobj=chunk_path,
                            path_in_repo=remote_file,
                            repo_id=self.repo_id,
                            repo_type="dataset",
                            revision=self.branch,
                        )
                        break
                    except Exception:
                        traceback.print_exc()
                        sleep(1)

                self.chunk_idx += 1
            except Exception:
                traceback.print_exc()
                raise
            finally:
                try:
                    os.remove(chunk_path)
                except FileNotFoundError:
                    pass

            if reached_end:
                break

    def _write_chunk_parquet(self, chunk_path: str) -> tuple[int, bool]:
        num_records = 0
        reached_end = False
        writer = None
        batch_rows = []
        schema = None

        try:
            while num_records < self.storage_chunk_size:
                row = self.queue.get()

                if row is None:
                    reached_end = True
                    # Preserve sentinel for the next chunk pass if this chunk already has rows.
                    if num_records > 0:
                        self.queue.put(None)
                    break

                if not row:
                    continue

                batch_rows.append(row)
                num_records += 1
                if len(batch_rows) >= self.memory_chunk_size:
                    table = pa.Table.from_pylist(batch_rows, schema=schema)
                    if writer is None:
                        schema = table.schema
                        writer = pq.ParquetWriter(
                            chunk_path,
                            schema,
                            compression='zstd',
                        )
                    writer.write_table(table)
                    batch_rows.clear()

            if batch_rows:
                table = pa.Table.from_pylist(batch_rows, schema=schema)
                if writer is None:
                    schema = table.schema
                    writer = pq.ParquetWriter(
                        chunk_path,
                        schema,
                        compression='zstd',
                    )
                writer.write_table(table)
                batch_rows.clear()
        finally:
            if writer is not None:
                writer.close()

        return num_records, reached_end

    def flush(self) -> int:
        """
        Stop uploader and push remaining rows.
        Safe to call multiple times; only first caller performs shutdown.
        """
        with self.closed.get_lock():
            if self.closed.value == 1:
                return 0
            self.closed.value = 1

        # First caller closes uploader.
        if self.uploader is None:
            return 0

        with self.write_lock:
            self.queue.put(None)

        self.uploader.join()
        if self.uploader.exitcode not in (0, None):
            raise RuntimeError(
                f"HF uploader process exited with non-zero code ({self.uploader.exitcode})."
            )
        self.uploader = None
        return 0

    def add_records(self, records: list[dict]) -> int:
        """
        Queue rows for uploader process.

        Returns:
        - Number of rows queued by this call.
        """
        if not records:
            return 0

        with self.closed.get_lock():
            if self.closed.value == 1:
                raise RuntimeError("Cannot add records after flush has started.")

        valid_records = [record for record in records if record]
        for record in valid_records:
            while True:
                try:
                    self.queue.put(record, timeout=1)
                    break
                except Full:
                    with self.closed.get_lock():
                        if self.closed.value == 1:
                            raise RuntimeError("Cannot add records after flush has started.")
                    continue

        return len(valid_records)

    def publish_wd_batch(self, items: list[dict]) -> int:
        """
        Map raw dump items using `item_to_json`, add to buffer,
        and upload only when a large enough shard accumulates.
        """
        rows = []
        for item in items:
            if item:
                rows.append({
                    "id": item["id"],
                    "labels": orjson.dumps(item["labels"]).decode("utf-8"),
                    "descriptions": orjson.dumps(item["descriptions"]).decode("utf-8"),
                    "aliases": orjson.dumps(item["aliases"]).decode("utf-8"),
                    "sitelinks": orjson.dumps(item["sitelinks"]).decode("utf-8"),
                    "claims": orjson.dumps(item["claims"]).decode("utf-8")
                })
        return self.add_records(rows)

    def publish_vector_batch(self, vectors: list[dict]) -> int:
        """
        Map cached vector rows and queue them for upload.
        """
        rows = []
        for vector in vectors:
            if vector and vector.get("vector") is not None:
                rows.append({
                    "id": vector.get("id") or "",
                    "vector": vector.get("vector"),
                    "lang": vector.get("lang") or "",
                    "wdid": vector.get("wdid") or "",
                })
        return self.add_records(rows)
