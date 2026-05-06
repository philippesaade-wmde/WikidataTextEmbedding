import json
import os
import traceback
import tempfile
import shutil
import gc
from time import sleep
from multiprocessing import Process, Queue, Value, Lock
from queue import Full
from datasets import Dataset
from huggingface_hub import HfApi, CommitOperationDelete


class WikidataHFDatasetPublisher:
    """
    Publish JSON-like records to a Hugging Face dataset repo in chunked splits
    using `Dataset.from_generator(...).push_to_hub(...)`.

    Designed for use inside `WikidataDumpReader.run(..., handler_receives_batch=True)`,
    while keeping local storage usage bounded.
    """

    def __init__(
        self,
        branch: str,
        config_path: str = None,
        chunk_size: int = 10_000,
        queue_size: int = 128,
        data_dir: str | None = 'data',
    ):
        self.chunk_size = max(1, int(chunk_size))
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
        while True:
            cache_dir = tempfile.mkdtemp(prefix="hf_gen_cache_")
            dataset = None
            try:
                dataset = Dataset.from_generator(
                    self._uploader_loop,
                    cache_dir=cache_dir,
                    keep_in_memory=False
                )

                if dataset.num_rows == 0:
                    break

                split_name = f"chunk_{self.chunk_idx}"
                while True:
                    try:
                        dataset.push_to_hub(
                            self.repo_id,
                            split=split_name,
                            data_dir=self.data_dir,
                            token=self.token,
                            revision=self.branch,
                        )
                        break
                    except Exception:
                        traceback.print_exc()
                        sleep(1)

                self.chunk_idx += 1
            except Exception:
                traceback.print_exc()
                sleep(1)
            finally:
                # Ensure temporary Arrow/cache files are removed after each chunk attempt.
                del dataset
                gc.collect()
                shutil.rmtree(cache_dir, ignore_errors=True)

    def _uploader_loop(self):
        num_records = 0
        while True:
            row = self.queue.get()
            if row is None:
                # If chunk is not empty, preserve sentinel for next pass
                if num_records > 0:
                    self.queue.put(None)
                break

            if not row:
                continue

            yield row

            num_records += 1
            if num_records >= self.chunk_size:
                break

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

        with self.write_lock:
            if self.closed.value == 1:
                raise RuntimeError("Cannot add records after flush has started.")

            for record in records:
                if record:
                    while True:
                        try:
                            self.queue.put(record, timeout=1)
                            break
                        except Full:
                            continue

        return len(records)

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
                    "labels": json.dumps(item["labels"], separators=(",", ":")),
                    "descriptions": json.dumps(item["descriptions"], separators=(",", ":")),
                    "aliases": json.dumps(item["aliases"], separators=(",", ":")),
                    "sitelinks": json.dumps(item["sitelinks"], separators=(",", ":")),
                    "claims": json.dumps(item["claims"], separators=(",", ":")),
                })
        return self.add_records(rows)

    def publish_vector_batch(self, vectors: list[dict]) -> int:
        """
        Map cached vector rows and queue them for upload.
        """
        rows = []
        for vector in vectors:
            if vector:
                rows.append({
                    "id": vector.get("id"),
                    "vector": vector.get("vector"),
                    "lang": vector.get("lang"),
                    "wdid": vector.get("wdid"),
                })
        return self.add_records(rows)
