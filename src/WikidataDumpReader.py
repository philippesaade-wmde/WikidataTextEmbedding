import gzip
import bz2
import os
import orjson
import time
from datetime import datetime, timezone
from tqdm import tqdm
import requests
import traceback
from multiprocessing import cpu_count, get_context
from queue import Full

class WikidataDumpReader:
    def __init__(
            self, file_path, num_processes=None,
            queue_size=100, skiplines=0, batch_size=100):
        """
        Initializes the reader with the file path, number of processes, queue size, and number of lines to skip.

        Parameters:
        - file_path (str): Path to the dump file.
        - num_processes (int): Number of consumer processes to spawn (default=4).
        - queue_size (int): Maximum size of the queue (default=100).
        - skiplines (int): Number of lines to skip at the beginning of the file (default=0).
        """
        self.file_path = file_path
        self.extension = file_path.split(".")[-1]
        self.skiplines = skiplines
        if not num_processes:
            num_processes = (cpu_count() or 2) - 1
        self.num_processes = max(1, num_processes)
        self.queue_size = queue_size
        self.batch_size = max(1, batch_size)

    def line_to_entity(self, line):
        """
        Converts a single line of text into a Wikidata entity (a dictionary).

        Parameters:
        - line (str): A single line representing a Wikidata entity in JSON format.

        Returns:
        - dict or None: The parsed entity if valid JSON, or None if empty or malformed.
        """
        line = line.strip("[] ,\n")
        if not line:
            return None

        try:
            entity = orjson.loads(line)
            return entity
        except ValueError as e:
            print("Failed to parse JSON:", e)
            traceback.print_exc()
            return None

    def run(self, handler_func, handler_receives_batch=False,
            max_iterations=None, verbose=True,
            init_consumer=None, init_consumer_args=None,
            consumer_join_timeout_s=3600):
        """
        Starts processing using a producer-consumer model with multiprocessing.

        Spawns:
        - 1 Producer process (reads lines from file, pushes to queue).
        - N Consumer processes (parse lines, call handler_func).
        - 1 Reporter process (optional, prints stats periodically if verbose=True).

        Parameters:
        - handler_func (callable): A function that takes a batch of parsed entity (list[dict]) as input.
        - handler_receives_batch (bool): If True, the handler function receives a batch of entities.
        - max_iterations (int or None): Stop after this many lines (if not None).
        - verbose (bool): If True, spawns a reporter process to print stats.
        - init_consumer (callable or None): Optional per-consumer initializer.
        - init_consumer_args (tuple or list or None): Optional args for initializer.
        """

        ctx = get_context("fork")
        init_consumer_args = tuple(init_consumer_args or ())

        self.queue = ctx.Queue(maxsize=self.queue_size) # This queue is shared across all processes
        self.iterations = ctx.Value('i', 0) # A counter for how many entities have been processed
        self.consumers_done = ctx.Value('i', 0) # How many consumers have received sentinel and exited
        self.handler_errors = ctx.Value('i', 0) # How many errors occurred in the handler function

        producer_p = ctx.Process(target=self._producer, args=(max_iterations,))
        consumer_ps = [
            ctx.Process(
                target=self._consumer,
                args=(
                    handler_func,
                    handler_receives_batch,
                    init_consumer,
                    init_consumer_args,
                )
            )
            for _ in range(self.num_processes)
        ]
        reporter_p = ctx.Process(target=self._reporter) if verbose else None

        try:
            # Start all processes
            producer_p.start()
            for cp in consumer_ps:
                cp.start()
            if reporter_p:
                reporter_p.start()

            while producer_p.is_alive():
                producer_p.join(timeout=0.2)
                failed_consumers = [
                    cp
                    for cp in consumer_ps
                    if cp.exitcode is not None
                ]
                if failed_consumers:
                    details = ", ".join(
                        f"pid={cp.pid} exitcode={cp.exitcode}"
                        for cp in failed_consumers
                    )
                    raise RuntimeError(
                        f"Consumer exited before producer completed ({details})"
                    )

            if producer_p.exitcode != 0:
                raise RuntimeError(f"Producer failed with exit code {producer_p.exitcode}")

            # Only running consumers need a shutdown sentinel.
            remaining = sum(1 for cp in consumer_ps if cp.is_alive())
            while remaining:
                try:
                    self.queue.put(None, timeout=1)
                    remaining -= 1
                except Full:
                    if not any(cp.is_alive() for cp in consumer_ps):
                        raise RuntimeError("All consumers died before shutdown sentinels could be queued")

            force_terminated = set()
            for cp in consumer_ps:
                cp.join(timeout=consumer_join_timeout_s)
                if cp.is_alive():
                    # Avoid deadlocking the parent forever on a stuck consumer.
                    force_terminated.add(cp.pid)
                    cp.terminate()
                    cp.join(timeout=5)
                if cp.exitcode != 0 and cp.pid not in force_terminated:
                    raise RuntimeError(f"Consumer failed with exit code {cp.exitcode}")

            if reporter_p:
                reporter_p.join(timeout=5)
                if reporter_p.is_alive():
                    reporter_p.terminate()
                    reporter_p.join(timeout=5)

        finally:
            # Ensure all processes are terminated
            if producer_p and producer_p.pid is not None:
                if producer_p.is_alive():
                    producer_p.terminate()
                producer_p.join(timeout=5)

            for cp in consumer_ps:
                if cp and cp.pid is not None:
                    if cp.is_alive():
                        cp.terminate()
                    cp.join(timeout=5)

            if reporter_p and reporter_p.pid is not None:
                if reporter_p.is_alive():
                    reporter_p.terminate()
                reporter_p.join(timeout=5)

    def _reporter(self, print_per_s=3):
        """
        Reports overall progress every few seconds until all consumers have exited.

        Parameters:
        - print_per_s: Number of seconds between each print.
        """

        start_time = time.time()

        with tqdm(desc="Processing items") as pbar:
            while True:
                time.sleep(print_per_s)

                with self.iterations.get_lock():
                    items_processed = self.iterations.value

                # Stop once every consumer has received a sentinel and exited.
                if self.consumers_done.value == self.num_processes:
                    break

                elapsed = time.time() - start_time
                rate = items_processed / elapsed if elapsed > 0 else 0.0

                # Update progress bar
                pbar.set_postfix_str(
                    f"Items Processed: {items_processed} "
                    f"| Processing Rate: {rate:.0f} items/sec"
                )
                pbar.update(items_processed - pbar.n)

            # Final update to ensure progress bar is complete
            pbar.update(items_processed - pbar.n)

    def _producer(self, max_iterations):
        """
        Reads lines from the file (plain or compressed) and puts them into the queue.

        Parameters:
        - max_iterations (int or None): If not None, stop reading after this many batches.
        """
        iters = 0
        if self.extension == 'json':
            lines_gen = self._read_jsonfile()
        elif self.extension in ['gz', 'bz2']:
            lines_gen = self._read_zipfile()
        else:
            raise ValueError(f"File extension '{self.extension}' is not supported")

        batch = []
        for line in lines_gen:
            batch.append(line)

            if len(batch) >= self.batch_size:
                iters += 1
                self.queue.put(batch)
                batch = []

                if max_iterations and iters >= max_iterations:
                    break

        if batch:
            self.queue.put(batch)

    def _consumer(self, handler_func, handler_receives_batch=False,
                  init_consumer=None, init_consumer_args=()):
        """
        Consumes lines from the queue, parses JSON, then invokes handler_func with the
        entity. Exits when receiving a sentinel (None).

        Parameters:
        - handler_func (callable): A function that takes a parsed entity (dict) as input.
        - handler_receives_batch (bool): If True, handler_func receives a batch of entities.
        - init_consumer (callable or None): Optional consumer initializer.
        - init_consumer_args (tuple): Args for the consumer initializer.
        """
        if init_consumer is not None:
            try:
                init_consumer(*init_consumer_args)
            except Exception:
                print("Consumer initializer failed")
                traceback.print_exc()
                raise

        while True:
            lines = self.queue.get()
            if lines is None:
                with self.consumers_done.get_lock():
                    self.consumers_done.value += 1
                break

            processed = 0
            if handler_receives_batch:
                entities = [e for line in lines \
                            if (e := self.line_to_entity(line)) is not None]
                if not entities:
                    continue

                try:
                    handler_func(entities)
                    processed = len(entities)
                except Exception as e:
                    # batch failed: fallback to item-granular processing
                    with self.handler_errors.get_lock():
                        self.handler_errors.value += 1
                    print(f"Batch handler failed, falling back per-item: {e}")
                    traceback.print_exc()

                    processed = 0
                    for entity in entities:
                        try:
                            handler_func([entity])
                            processed += 1
                        except Exception as item_e:
                            with self.handler_errors.get_lock():
                                self.handler_errors.value += 1
                            print(f"Item handler failed: {item_e}")
                            traceback.print_exc()
            else:
                processed = 0
                for line in lines:
                    e = self.line_to_entity(line)
                    if e is None:
                        continue

                    try:
                        handler_func(e)
                        processed += 1
                    except Exception as e:
                        with self.handler_errors.get_lock():
                            self.handler_errors.value += 1
                        print(f"Handler failed: {e}")
                        traceback.print_exc()
                        continue

            if processed > 0:
                with self.iterations.get_lock():
                    self.iterations.value += processed

    def _read_jsonfile(self):
        """
        Yields lines from a .json file, skipping self.skiplines lines
            at the start.

        Returns:
        - Iterator[str]: An iterator over lines from the JSON file.
        """
        file = None
        try:
            file = open(self.file_path, mode="r", encoding="utf-8")
            # Skip lines if requested
            for _ in tqdm(range(self.skiplines), desc="Skipping lines"):
                file.readline()

            for line in file:
                if not line:
                    break
                yield line
        finally:
            if file:
                file.close()

    def _read_zipfile(self):
        """
        Yields lines from a .gz or .bz2 file, skipping self.skiplines lines at the start.

        Returns:
        - Iterator[str]: An iterator over lines from the compressed file.
        """
        file = None
        try:
            if self.extension == 'gz':
                file = gzip.open(self.file_path, mode="rt", encoding="utf-8")
            elif self.extension == 'bz2':
                file = bz2.open(self.file_path, mode="rt", encoding="utf-8")
            else:
                raise ValueError(f"Unsupported extension '{self.extension}'")

            for _ in tqdm(range(self.skiplines), desc="Skipping lines"):
                file.readline()

            for line in file:
                if not line:
                    break
                yield line
        finally:
            if file:
                file.close()

    def download(self, show_progress=True, chunk_size=1024 * 1024):
        """
        Downloads the latest Wikidata dump file with streaming.
        Writes a sidecar {file_path}.date file containing the dump date (YYYY-MM-DD)
        taken from the server's Last-Modified header.

        Parameters:
        - show_progress (bool): If True, displays a download progress bar.
        - chunk_size (int): Number of bytes to read per chunk.
        """
        url = f"https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.{self.extension}"

        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))

            last_modified = response.headers.get("Last-Modified", "")
            try:
                dump_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                dump_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            with open(self.file_path, "wb") as f, tqdm(
                total=total_size if total_size > 0 else None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {self.file_path}",
                disable=not show_progress,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))

        with open(self.file_path + ".date", "w") as f:
            f.write(dump_date)

    def get_dump_date(self):
        """
        Returns the dump date as a 'YYYY-MM-DD' string.
        Reads from the sidecar .date file written during download, or falls back
        to the file's modification time if the sidecar is absent.
        """
        date_file = self.file_path + ".date"
        if os.path.exists(date_file):
            with open(date_file) as f:
                return f.read().strip()
        mtime = os.path.getmtime(self.file_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%d")
