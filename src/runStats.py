from datetime import datetime, timezone
import json
import os
from multiprocessing import get_context


class RunStatsTracker:
    def __init__(self, output_path: str, config: dict):
        self.ctx = get_context("fork")
        self.output_path = output_path
        self.stats = {
            "started_at": self._utc_now_iso(),
            "finished_at": None,
            "status": "running",
            "config": config,
            "stages": {
                "labels": {},
                "wd_to_hf": {},
                "vectors_by_language": {},
            },
            "errors": {
                "total": 0,
                "by_stage": {},
                "exceptions": [],
            },
        }
        self._active_counters = None

    @staticmethod
    def _utc_now_iso():
        return datetime.now(timezone.utc).isoformat()

    def _create_counters(self, counter_names):
        return {name: self.ctx.Value("i", 0) for name in counter_names}

    def _read_counters(self, counters):
        return {name: int(counter.value) for name, counter in counters.items()}

    def start_counters(self, counter_names):
        self._active_counters = self._create_counters(counter_names)
        return self._active_counters

    def clear_counters(self):
        self._active_counters = None

    def counter_add(self, name, value):
        if not self._active_counters:
            return

        counter = self._active_counters.get(name)
        if counter is None:
            return

        with counter.get_lock():
            counter.value += int(value)

    def read_counters(self, counters):
        return self._read_counters(counters)

    def record_error(self, stage_name, count=1, exc=None):
        count = int(count)
        if count <= 0:
            return

        self.stats["errors"]["total"] += count
        errors_by_stage = self.stats["errors"]["by_stage"]
        errors_by_stage[stage_name] = errors_by_stage.get(stage_name, 0) + count

        if exc is not None:
            self.stats["errors"]["exceptions"].append({
                "stage": stage_name,
                "type": type(exc).__name__,
                "message": str(exc),
            })

    def set_stage_stats(self, stage_name, stage_stats):
        self.stats["stages"][stage_name] = stage_stats

    def set_language_stats(self, lang, lang_stats):
        self.stats["stages"]["vectors_by_language"][lang] = lang_stats

    def get_language_stats(self, lang, defaults=None):
        lang_stats = self.stats["stages"]["vectors_by_language"].setdefault(lang, {})
        if defaults:
            lang_stats.update(defaults)
        return lang_stats

    def add_summary(self):
        languages_stats = self.stats["stages"].get("vectors_by_language", {}).values()
        total_vector_create_items = 0
        total_vector_update_items = 0
        total_vector_saved_docs = 0
        total_vector_hf_rows = 0

        for lang_stats in languages_stats:
            vectordb_stats = lang_stats.get("vectordb", {})
            total_vector_create_items += int(vectordb_stats.get("vector_create_items", 0))
            total_vector_update_items += int(vectordb_stats.get("vector_update_items", 0))
            total_vector_saved_docs += int(vectordb_stats.get("vector_saved_docs", 0))

            vectors_to_hf_stats = lang_stats.get("vectors_to_hf", {})
            total_vector_hf_rows += int(vectors_to_hf_stats.get("rows_pushed", 0))

        self.stats["summary"] = {
            "wd_hf_rows_pushed": int(self.stats["stages"].get("wd_to_hf", {}).get("wd_hf_rows", 0)),
            "vector_create_items": total_vector_create_items,
            "vector_update_items": total_vector_update_items,
            "vector_saved_docs": total_vector_saved_docs,
            "vector_hf_rows_pushed": total_vector_hf_rows,
            "total_errors": int(self.stats["errors"].get("total", 0)),
        }

    def write(self):
        self.stats["finished_at"] = self._utc_now_iso()
        stats_dir = os.path.dirname(self.output_path)
        if stats_dir:
            os.makedirs(stats_dir, exist_ok=True)

        with open(self.output_path, "w", encoding="utf-8") as f_out:
            json.dump(self.stats, f_out, ensure_ascii=False, indent=2)

    def finalize(self, status):
        self.stats["status"] = status
        self.add_summary()
        self.write()
