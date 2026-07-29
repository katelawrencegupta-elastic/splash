#!/usr/bin/env python3
"""Suggest Splash pipeline shard count from daily volume + event size + peak factor.

Usage:
  python suggest_shards.py --tb-day 1 --event-bytes 1536 --peak-factor 2
  python suggest_shards.py --gb-day 500 --event-bytes 512 --peak-factor 3

Formula (docs/runbooks/sharding.md):
  shards ≈ ceil(ceil(peak_GBps / 0.008) * 1.25)
  peak_GBps = (gb_day / 86400) * peak_factor
"""

from __future__ import annotations

import argparse
import math


PLAN_GBPS = 0.008
SEC_PER_DAY = 86400


def plan_shards(peak_gbps: float) -> int:
    return math.ceil(math.ceil(peak_gbps / PLAN_GBPS) * 1.25)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--tb-day", type=float, help="Daily volume in TB (decimal, 1 TB = 1000 GB)")
    g.add_argument("--gb-day", type=float, help="Daily volume in GB")
    p.add_argument("--event-bytes", type=int, default=1536, help="P50 event size (default 1536)")
    p.add_argument("--peak-factor", type=float, default=2.0, help="Peak/avg ratio (default 2)")
    args = p.parse_args()

    gb_day = args.gb_day if args.gb_day is not None else args.tb_day * 1000.0
    avg_gbps = gb_day / SEC_PER_DAY
    peak_gbps = avg_gbps * args.peak_factor
    shards = plan_shards(peak_gbps)
    avg_eps = (avg_gbps * 1e9) / max(args.event_bytes, 1)
    peak_eps = (peak_gbps * 1e9) / max(args.event_bytes, 1)

    print(f"daily_gb={gb_day:.1f}")
    print(f"event_bytes={args.event_bytes}")
    print(f"peak_factor={args.peak_factor}")
    print(f"avg_gbps={avg_gbps:.6f}  peak_gbps={peak_gbps:.6f}")
    print(f"avg_eps≈{avg_eps:.0f}  peak_eps≈{peak_eps:.0f}")
    print(f"suggested_pipeline_shards={shards}")
    print(
        "note: if event_bytes ≠ 1536, re-run loadtest S1_512 / S1_1536 / S1_4096 "
        "before locking capacity (GB/s floor is ~0.008; CPU/GB rises for smaller events)."
    )


if __name__ == "__main__":
    main()
