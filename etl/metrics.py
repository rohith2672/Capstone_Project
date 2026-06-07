"""RunMetrics: the single accumulator that feeds both ETL_AUDIT_LOG and the
data quality report. One instance is created per pipeline run and threaded through
every phase of WebLogProcessor.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

Status = Literal["RUNNING", "SUCCESS", "PARTIAL", "FAILED"]


@dataclass
class RunMetrics:
    etl_run_id: str
    etl_run_date: str
    etl_run_timestamp: str

    extracted: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    quarantined: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    # loaded[layer][table] = row count — keyed by layer because ETL_AUDIT_LOG has one
    # row per (run, layer); see to_audit_rows().
    loaded: dict[str, dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    rejection_reason_counts: dict[str, dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    quarantine_paths: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    session_anomaly_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    quality_observations: dict[str, float] = field(default_factory=dict)
    validation_results: list[dict] = field(default_factory=list)

    status: Status = "RUNNING"
    error_message: str | None = None

    # ------------------------------------------------------------------
    def record_extracted(self, source: str, n: int) -> None:
        self.extracted[source] += n

    def record_quarantined(self, source: str, n: int, reasons: pd.Series | None = None) -> None:
        self.quarantined[source] += n
        if reasons is not None and len(reasons):
            counts = reasons.value_counts()
            for reason, count in counts.items():
                self.rejection_reason_counts[source][str(reason)] += int(count)

    def record_quarantine_path(self, source: str, path: str) -> None:
        self.quarantine_paths[source].append(path)

    def record_loaded(self, layer: str, table: str, n: int) -> None:
        self.loaded[layer][table] += n

    def record_session_anomaly(self, kind: str, n: int) -> None:
        self.session_anomaly_counts[kind] += n

    def set_quality_observations(self, observations: dict[str, float]) -> None:
        self.quality_observations.update(observations)

    def set_validation_results(self, results: list[dict]) -> None:
        self.validation_results = results

    def finalize(self, status: Status, error: str | None = None) -> None:
        self.status = status
        self.error_message = error

    # ------------------------------------------------------------------
    def quarantine_paths_flat(self) -> str:
        paths = [p for plist in self.quarantine_paths.values() for p in plist]
        return ";".join(paths)

    def total_rows_extracted(self) -> int:
        return sum(self.extracted.values())

    def total_rows_quarantined(self) -> int:
        return sum(self.quarantined.values())

    def total_rows_loaded(self, layer: str | None = None) -> int:
        if layer is not None:
            return sum(self.loaded.get(layer, {}).values())
        return sum(sum(tables.values()) for tables in self.loaded.values())

    def to_audit_rows(self, *, source_file: str) -> list[dict]:
        """One row per Medallion layer — matches the spec's ETL_AUDIT_LOG schema, whose
        `layer VARCHAR(20) -- 'bronze', 'silver', 'gold'` column implies a row per layer
        per run. Extraction/quarantine totals are attributed to 'bronze' (the only layer
        that reads source files and rejects rows in this design — see README Assumptions);
        silver/gold rows report their own load counts with extracted/quarantined as 0.
        """
        rows = []
        for layer in ("bronze", "silver", "gold"):
            is_bronze = layer == "bronze"
            rows.append(
                {
                    "etl_run_id": self.etl_run_id,
                    "etl_run_date": self.etl_run_date,
                    "etl_run_timestamp": self.etl_run_timestamp,
                    "source_file": source_file,
                    "layer": layer,
                    "rows_extracted": self.total_rows_extracted() if is_bronze else 0,
                    "rows_quarantined": self.total_rows_quarantined() if is_bronze else 0,
                    "rows_loaded": self.total_rows_loaded(layer),
                    "quarantine_s3_path": self.quarantine_paths_flat() if is_bronze else "",
                    "status": self.status,
                    "error_message": self.error_message,
                }
            )
        return rows

    def as_dict(self) -> dict:
        return {
            "etl_run_id": self.etl_run_id,
            "etl_run_date": self.etl_run_date,
            "etl_run_timestamp": self.etl_run_timestamp,
            "status": self.status,
            "error_message": self.error_message,
            "extracted": dict(self.extracted),
            "quarantined": dict(self.quarantined),
            "loaded": {layer: dict(tables) for layer, tables in self.loaded.items()},
            "rejection_reason_counts": {k: dict(v) for k, v in self.rejection_reason_counts.items()},
            "quarantine_paths": dict(self.quarantine_paths),
            "session_anomaly_counts": dict(self.session_anomaly_counts),
            "quality_observations": self.quality_observations,
            "validation_results": self.validation_results,
        }
