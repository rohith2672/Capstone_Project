"""Render and persist the data_quality_report.md deliverable from a finished
RunMetrics — the same accumulator that feeds ETL_AUDIT_LOG, so the report and the
audit trail can never drift apart.
"""
from __future__ import annotations

import os

from etl.metrics import RunMetrics


def render_data_quality_report(metrics: RunMetrics) -> str:
    """Pure rendering function (no I/O) — build the report's Markdown text from a
    RunMetrics snapshot. Kept separate from write_report() so the content can be
    unit-tested without touching the filesystem."""
    d = metrics.as_dict()
    lines: list[str] = []

    lines += [
        f"# Data Quality Report — Run `{d['etl_run_id']}`",
        "",
        f"- **Run date:** {d['etl_run_date']}",
        f"- **Run timestamp:** {d['etl_run_timestamp']}",
        f"- **Status:** {d['status']}",
    ]
    if d["error_message"]:
        lines.append(f"- **Error:** {d['error_message']}")
    lines.append("")

    lines += ["## Row Counts", "", "### Extraction & Quarantine by Source", "", "| Source | Extracted | Quarantined |", "|---|---:|---:|"]
    for source in sorted(d["extracted"]):
        lines.append(f"| {source} | {d['extracted'][source]} | {d['quarantined'].get(source, 0)} |")

    lines += ["", "### Rows Loaded by Layer", "", "| Layer | Dataset | Rows Loaded |", "|---|---|---:|"]
    for layer in ("bronze", "silver", "gold"):
        for dataset, n in d["loaded"].get(layer, {}).items():
            lines.append(f"| {layer} | {dataset} | {n} |")
    lines.append("")

    lines += ["## Quarantine Breakdown", ""]
    if d["quarantined"]:
        lines += ["| Source | Rejection Reason | Count |", "|---|---|---:|"]
        for source, reasons in d["rejection_reason_counts"].items():
            for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
                lines.append(f"| {source} | {reason} | {count} |")
        lines.append("")
        lines.append("**Quarantine object paths:**")
        for source, paths in d["quarantine_paths"].items():
            for p in paths:
                lines.append(f"- `{source}`: `{p}`")
    else:
        lines.append("_No rows were quarantined this run._")
    lines.append("")

    lines += ["## Data Quality Observations", ""]
    if d["quality_observations"]:
        lines += ["| Observation | Rate |", "|---|---:|"]
        for k, v in sorted(d["quality_observations"].items()):
            lines.append(f"| {k} | {v:.2%} |")
    else:
        lines.append("_None recorded._")
    lines.append("")

    lines += ["## Session-Level Anomalies", ""]
    if d["session_anomaly_counts"]:
        lines += ["| Anomaly | Sessions Flagged |", "|---|---:|"]
        for k, v in sorted(d["session_anomaly_counts"].items()):
            lines.append(f"| {k} | {v} |")
    else:
        lines.append("_None recorded._")
    lines.append("")

    lines += ["## Post-Load Validation Results", ""]
    if d["validation_results"]:
        lines += ["| Check | Status | Detail |", "|---|---|---|"]
        for r in d["validation_results"]:
            detail = r.get("result") if r.get("result") is not None else r.get("note", "")
            lines.append(f"| {r['check']} | {r['status']} | {detail} |")
    else:
        lines.append("_Skipped — no Snowflake connection (dry-run / local-only run)._")
    lines.append("")

    lines += ["## Recommendations", ""]
    lines += _recommendations(d)
    lines.append("")

    return "\n".join(lines)


def _recommendations(d: dict) -> list[str]:
    """Derive a short list of human-actionable recommendations from the run's
    metrics — purely rule-based thresholds, no ML, intentionally simple and explainable."""
    recs: list[str] = []

    total_extracted = sum(d["extracted"].values())
    total_quarantined = sum(d["quarantined"].values())
    quarantine_rate = (total_quarantined / total_extracted) if total_extracted else 0.0

    if quarantine_rate > 0.10:
        recs.append(
            f"- Quarantine rate is **{quarantine_rate:.1%}** of extracted rows — "
            f"investigate upstream data quality at the source system(s) before the next run."
        )
    else:
        recs.append(f"- Quarantine rate ({quarantine_rate:.1%}) is within normal bounds; continue routine monitoring.")

    abandoned = d["session_anomaly_counts"].get("abandoned_cart", 0)
    if abandoned:
        recs.append(f"- **{abandoned}** abandoned-cart sessions detected — candidates for retargeting/marketing analysis.")

    long_sessions = d["session_anomaly_counts"].get("long_session", 0)
    if long_sessions:
        recs.append(
            f"- **{long_sessions}** unusually long sessions flagged (> 2 std. dev. above the mean duration) — "
            f"review for bot traffic, idle tabs, or instrumentation issues."
        )

    high_activity = d["session_anomaly_counts"].get("high_activity", 0)
    if high_activity:
        recs.append(f"- **{high_activity}** high-activity sessions (> 50 actions) flagged — review for scraping/automation.")

    if d["status"] == "FAILED":
        recs.append("- This run **FAILED** — see the error message above; re-run after resolving the root cause.")

    return recs


def write_report(metrics: RunMetrics, *, settings) -> tuple[str, str]:
    """Persist the rendered report to BOTH:
    - the project root `data_quality_report.md` (overwritten each run — the "current" deliverable)
    - `<settings.report_dir>/data_quality_report_<date>_<run_id>.md` (timestamped history)

    Returns (root_path, history_path).
    """
    text = render_data_quality_report(metrics)

    root_path = "data_quality_report.md"
    with open(root_path, "w", encoding="utf-8") as f:
        f.write(text)

    os.makedirs(settings.report_dir, exist_ok=True)
    history_path = os.path.join(
        settings.report_dir,
        f"data_quality_report_{metrics.etl_run_date}_{metrics.etl_run_id}.md",
    )
    with open(history_path, "w", encoding="utf-8") as f:
        f.write(text)

    return root_path, history_path
