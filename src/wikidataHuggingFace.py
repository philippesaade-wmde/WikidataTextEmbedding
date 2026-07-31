import json
import os
import re
import tempfile
import traceback
from multiprocessing import Lock, Process, Queue, Value
from queue import Full
from time import sleep

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import (
    CommitOperationAdd,
    CommitOperationCopy,
    CommitOperationDelete,
    HfApi,
    hf_hub_download,
)
from huggingface_hub.errors import HfHubHTTPError

from src.hfUploadCheckpoint import HFUploadCheckpoint


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

        self.create_new_branch()
        self.checkpoint = HFUploadCheckpoint(
            branch=self.branch,
        )
        self.chunk_idx = self._next_remote_chunk_idx()
        print(
            f"HF checkpoint: {self.checkpoint.path} "
            f"(next chunk {self.chunk_idx})",
            flush=True,
        )

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
            self._create_commit_with_retry(
                api,
                repo_id=self.repo_id,
                repo_type="dataset",
                revision=self.branch,
                operations=ops,
                commit_message="Remove data directory from branch",
            )

        return True

    @staticmethod
    def _create_commit_with_retry(api: HfApi, **kwargs):
        sleep_time = 1
        while True:
            try:
                return api.create_commit(**kwargs)
            except HfHubHTTPError as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                if status_code is not None and status_code != 429 and status_code < 500:
                    raise

                retry_after = None
                headers = getattr(response, "headers", {}) or {}
                if "retry-after" in headers:
                    try:
                        retry_after = int(headers["retry-after"])
                    except ValueError:
                        retry_after = None

                wait_s = max(retry_after or sleep_time, 1)
                print(f"HF commit failed/rate-limited; retrying in {wait_s} seconds.", flush=True)
                sleep(wait_s)
                sleep_time = min(sleep_time * 2, 3600 if status_code == 429 else 30)
            except Exception:
                traceback.print_exc()
                sleep(sleep_time)
                sleep_time = min(sleep_time * 2, 30)

    @staticmethod
    def merge_to_main(
        branch: str,
        config_path: str = None,
        batch_size: int = 1000,
    ) -> dict:
        """
        Make main match a published branch without downloading parquet chunks.

        Parquet files are copied server-side. Regular small files, such as README.md
        and .gitattributes, are downloaded and uploaded.
        """
        target_branch = "main"
        token = os.environ.get("HF_TOKEN")
        repo_id = os.environ.get("HF_REPO_ID")
        if config_path and os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f_in:
                data = json.load(f_in)
                token = data.get("API_KEY", token)
                repo_id = data.get("REPO_ID", repo_id)

        if not token:
            raise ValueError("Hugging Face API token not found.")
        if not repo_id:
            raise ValueError("Hugging Face repository ID not found.")
        if not branch:
            raise ValueError("Source Hugging Face branch is not set.")
        if branch == target_branch:
            print(f"HF branch is already {target_branch}; merge skipped.", flush=True)
            return {
                "repo_id": repo_id,
                "source_branch": branch,
                "target_branch": target_branch,
                "files_copied": 0,
                "files_uploaded": 0,
                "files_deleted": 0,
                "commits": 0,
                "skipped": True,
            }

        api = HfApi(token=token)
        source_files = set(api.list_repo_files(
            repo_id=repo_id,
            repo_type="dataset",
            revision=branch,
        ))
        target_files = set(api.list_repo_files(
            repo_id=repo_id,
            repo_type="dataset",
            revision=target_branch,
        ))

        files_to_delete = sorted(target_files - source_files)
        files_to_copy = sorted(
            path
            for path in source_files
            if path.endswith(".parquet")
        )
        files_to_add = sorted(
            path
            for path in source_files
            if not path.endswith(".parquet")
        )
        batch_size = max(1, int(batch_size))
        commits = 0

        print(
            f"Merging HF branch {branch} -> {target_branch} "
            f"{len(files_to_copy)} LFS copies, {len(files_to_add)} small-file uploads, "
            f"{len(files_to_delete)} deletes",
            flush=True,
        )

        for start in range(0, len(files_to_delete), batch_size):
            batch = files_to_delete[start:start + batch_size]
            WikidataHFDatasetPublisher._create_commit_with_retry(
                api,
                repo_id=repo_id,
                repo_type="dataset",
                revision=target_branch,
                operations=[CommitOperationDelete(path_in_repo=path) for path in batch],
                commit_message=f"Delete stale files before merging {branch}",
            )
            commits += 1

        with tempfile.TemporaryDirectory(prefix="hf_merge_") as tmp_dir:
            for start in range(0, len(files_to_add), batch_size):
                batch = files_to_add[start:start + batch_size]
                operations = []
                for path in batch:
                    local_path = hf_hub_download(
                        repo_id=repo_id,
                        repo_type="dataset",
                        revision=branch,
                        filename=path,
                        token=token,
                        local_dir=tmp_dir,
                    )
                    operations.append(
                        CommitOperationAdd(
                            path_in_repo=path,
                            path_or_fileobj=local_path,
                        )
                    )
                WikidataHFDatasetPublisher._create_commit_with_retry(
                    api,
                    repo_id=repo_id,
                    repo_type="dataset",
                    revision=target_branch,
                    operations=operations,
                    commit_message=f"Merge small files from {branch} into {target_branch}",
                )
                commits += 1

        for start in range(0, len(files_to_copy), batch_size):
            batch = files_to_copy[start:start + batch_size]
            WikidataHFDatasetPublisher._create_commit_with_retry(
                api,
                repo_id=repo_id,
                repo_type="dataset",
                revision=target_branch,
                operations=[
                    CommitOperationCopy(
                        src_path_in_repo=path,
                        path_in_repo=path,
                        src_revision=branch,
                    )
                    for path in batch
                ],
                commit_message=f"Merge {branch} into {target_branch}",
            )
            commits += 1

        return {
            "repo_id": repo_id,
            "source_branch": branch,
            "target_branch": target_branch,
            "files_copied": len(files_to_copy),
            "files_uploaded": len(files_to_add),
            "files_deleted": len(files_to_delete),
            "commits": commits,
            "skipped": False,
        }

    def _next_remote_chunk_idx(self):
        api = HfApi(token=self.token)
        remote_base = (self.data_dir or "data").rstrip("/")
        pattern = re.compile(rf"^{re.escape(remote_base)}/chunk_(\d+)\.parquet$")
        files = api.list_repo_files(
            repo_id=self.repo_id,
            repo_type="dataset",
            revision=self.branch,
        )
        chunk_indices = [
            int(match.group(1))
            for f in files
            if (match := pattern.match(f))
        ]
        return max(chunk_indices, default=-1) + 1

    def _publish_records(self):
        api = HfApi(token=self.token)
        while True:
            chunk_fd, chunk_path = tempfile.mkstemp(prefix="hf_chunk_", suffix=".parquet")
            os.close(chunk_fd)

            num_records = 0
            reached_end = False
            try:
                num_records, reached_end, chunk_ids = self._write_chunk_parquet(chunk_path)

                if num_records == 0:
                    if reached_end:
                        break
                    continue

                remote_base = (self.data_dir or "data").rstrip("/")
                remote_file = f"{remote_base}/chunk_{self.chunk_idx}.parquet"
                sleep_time = 1
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
                        sleep(sleep_time)
                        sleep_time = min(sleep_time * 2, 30)

                self.checkpoint.add_ids(chunk_ids)
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

    def _write_chunk_parquet(self, chunk_path: str) -> tuple[int, bool, list[str]]:
        num_records = 0
        reached_end = False
        chunk_ids = []
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
                chunk_ids.append(row.get("id") or "")
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

        return num_records, reached_end, chunk_ids

    def existing_ids(self, ids: list[str]) -> set[str]:
        return self.checkpoint.existing_ids(ids)

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
