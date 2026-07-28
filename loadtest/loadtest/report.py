"""Summarize a load-test run from observer CSV + generator stats."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _f(row: dict[str, str], key: str) -> float | None:
    v = row.get(key, "")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def summarize(
    *,
    metrics_csv: Path,
    gen_snapshot: dict[str, Any],
    scenario_id: str,
    phase: str,
) -> dict[str, Any]:
    rows: list[dict[str, str]] = []
    if metrics_csv.exists():
        with metrics_csv.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))

    queue_vals = [v for r in rows if (v := _f(r, "s2s_upstream_queue")) is not None]
    eps_vals = [v for r in rows if (v := _f(r, "gen_eps")) is not None]
    es_first = _f(rows[0], "es_count") if rows else None
    es_last = _f(rows[-1], "es_count") if rows else None
    es_delta = (
        (es_last - es_first) if es_first is not None and es_last is not None else None
    )

    sent = int(gen_snapshot.get("sent_events") or 0)
    elapsed = float(gen_snapshot.get("elapsed_s") or 0.0)
    sent_bytes = int(gen_snapshot.get("sent_bytes") or 0)

    queue_pegged = bool(queue_vals) and sum(1 for q in queue_vals if q >= 9500) >= 3
    classify_fail = any(r.get("classify_ok") == "False" for r in rows)

    index_ratio = None
    if es_delta is not None and sent > 0:
        index_ratio = es_delta / sent

    passed = (
        sent > 0
        and not queue_pegged
        and not classify_fail
        and (index_ratio is None or index_ratio >= 0.95)
    )

    summary: dict[str, Any] = {
        "scenario": scenario_id,
        "phase": phase,
        "passed": passed,
        "sent_events": sent,
        "sent_bytes": sent_bytes,
        "elapsed_s": round(elapsed, 3),
        "avg_eps": round(sent / elapsed, 2) if elapsed > 0 else 0.0,
        "avg_gbps_payload": round(sent_bytes / elapsed / 1e9, 6) if elapsed > 0 else 0.0,
        "gen_errors": gen_snapshot.get("errors", 0),
        "observe_samples": len(rows),
        "upstream_queue_max": max(queue_vals) if queue_vals else None,
        "upstream_queue_median": (
            sorted(queue_vals)[len(queue_vals) // 2] if queue_vals else None
        ),
        "gen_eps_median": (
            sorted(eps_vals)[len(eps_vals) // 2] if eps_vals else None
        ),
        "es_count_delta": es_delta,
        "index_ratio": round(index_ratio, 4) if index_ratio is not None else None,
        "queue_pegged": queue_pegged,
        "classify_fail": classify_fail,
        "notes": [],
    }
    if queue_pegged:
        summary["notes"].append("upstream_queue pegged near 10k — Logstash/ES behind")
    if classify_fail:
        summary["notes"].append("classify /health not ok during run")
    if index_ratio is not None and index_ratio < 0.95:
        summary["notes"].append(
            f"ES indexed ratio {index_ratio:.3f} < 0.95 (allow drain lag)"
        )
    if es_delta is None:
        summary["notes"].append("ES _count not configured — end-to-end ratio skipped")
    return summary


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
