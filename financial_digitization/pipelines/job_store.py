from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    status: str
    attempts: int
    payload: dict[str, object]
    error: str = ""


class PersistentJobStore:
    """File-backed job store with metrics, retries, and DLQ support."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.jobs_dir = root / "jobs"
        self.dlq_dir = root / "dlq"
        self.metrics_file = root / "metrics.json"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        if not self.metrics_file.exists():
            self._write_metrics({"submitted": 0, "succeeded": 0, "failed": 0, "retried": 0, "dlq": 0})

    def upsert(self, record: JobRecord) -> None:
        path = self.jobs_dir / f"{record.job_id}.json"
        path.write_text(json.dumps(record.__dict__, indent=2), encoding="utf-8")

    def get(self, job_id: str) -> JobRecord | None:
        path = self.jobs_dir / f"{job_id}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return JobRecord(**data)

    def move_to_dlq(self, record: JobRecord) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dlq_path = self.dlq_dir / f"{stamp}_{record.job_id}.json"
        dlq_path.write_text(json.dumps(record.__dict__, indent=2), encoding="utf-8")
        self.bump_metric("dlq")

    def bump_metric(self, key: str, amount: int = 1) -> None:
        metrics = self.read_metrics()
        metrics[key] = int(metrics.get(key, 0)) + amount
        self._write_metrics(metrics)

    def read_metrics(self) -> dict[str, int]:
        return json.loads(self.metrics_file.read_text(encoding="utf-8"))

    def _write_metrics(self, metrics: dict[str, int]) -> None:
        self.metrics_file.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
