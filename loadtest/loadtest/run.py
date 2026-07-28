"""CLI: run Splash load-test scenarios."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

import yaml

from .gen_cooked import run_cooked
from .gen_uncooked import run_uncooked
from .observe import Observer, ObserverConfig
from .report import summarize, write_summary
from .stats import GenStats

logger = logging.getLogger("loadtest")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCENARIOS = ROOT / "scenarios.yaml"
DEFAULT_RESULTS = ROOT / "results"


def _load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise SystemExit(f"invalid scenarios file: {path}")
    return data


def _merge_settings(
    cfg: dict[str, Any], scenario_id: str, args: argparse.Namespace
) -> dict[str, Any]:
    defaults = dict(cfg.get("defaults") or {})
    scenarios = cfg.get("scenarios") or {}
    if scenario_id not in scenarios:
        known = ", ".join(sorted(scenarios))
        raise SystemExit(f"unknown scenario {scenario_id!r}; choose from: {known}")
    sc = dict(scenarios[scenario_id])
    settings = {**defaults, **sc}

    if args.eps is not None:
        settings["target_eps"] = args.eps
    if args.duration is not None:
        settings["duration"] = args.duration
    if args.connections is not None:
        settings["connections"] = args.connections
    if args.event_bytes is not None:
        settings["event_bytes"] = args.event_bytes
    if args.skip_warm:
        settings["warm_seconds"] = 0
    if args.skip_burst:
        settings["burst_seconds"] = 0

    # Env overrides for ES verification
    settings["elastic_host"] = (
        args.elastic_host
        or settings.get("elastic_host")
        or os.environ.get("ELASTIC_HOST", "")
    )
    settings["elastic_api_key"] = (
        args.elastic_api_key
        or settings.get("elastic_api_key")
        or os.environ.get("ELASTIC_API_KEY", "")
    )
    if args.namespace:
        settings["namespace"] = args.namespace
    return settings


async def _observe_loop(
    observer: Observer,
    stats: GenStats,
    stop: asyncio.Event,
    interval_s: float,
) -> None:
    while not stop.is_set():
        row = await observer.sample(gen=stats.snapshot())
        logger.info(
            "t=%.0fs eps=%.0f queue=%s classify_ok=%s es_count=%s",
            row.get("t_mono") or 0,
            float(row.get("gen_eps") or 0),
            row.get("s2s_upstream_queue"),
            row.get("classify_ok"),
            row.get("es_count"),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


async def _run_phase(
    *,
    settings: dict[str, Any],
    eps: float,
    duration_s: float,
    phase: str,
    out_dir: Path,
    scenario_id: str,
) -> dict[str, Any]:
    if duration_s <= 0:
        return {"phase": phase, "skipped": True}

    path = settings.get("path", "cooked")
    hot_fraction = float(settings.get("hot_fraction", 1.0))
    connections = int(settings.get("connections", 8))
    event_bytes = int(settings.get("event_bytes", 1536))
    interval_s = float(settings.get("observe_interval_s", 5.0))
    stats = GenStats()

    stop_gen = asyncio.Event()
    stop_obs = asyncio.Event()
    metrics_csv = out_dir / f"metrics_{phase}.csv"

    obs_cfg = ObserverConfig(
        s2s_health_url=str(settings.get("s2s_health_url")),
        classify_health_url=str(settings.get("classify_health_url")),
        logstash_stats_url=str(settings.get("logstash_stats_url")),
        elastic_host=str(settings.get("elastic_host") or ""),
        elastic_api_key=str(settings.get("elastic_api_key") or ""),
        namespace=str(settings.get("namespace") or "loadtest"),
        interval_s=interval_s,
    )

    logger.info(
        "phase=%s path=%s eps=%.0f duration=%.0fs hot_fraction=%.2f connections=%d",
        phase,
        path,
        eps,
        duration_s,
        hot_fraction,
        connections,
    )

    async with Observer(obs_cfg, metrics_csv) as observer:
        obs_task = asyncio.create_task(
            _observe_loop(observer, stats, stop_obs, interval_s),
            name=f"observe-{phase}",
        )
        try:
            if path == "cooked":
                await run_cooked(
                    host=str(settings.get("cooked_host", "127.0.0.1")),
                    port=int(settings.get("cooked_port", 39998)),
                    eps=eps,
                    duration_s=duration_s,
                    connections=connections,
                    hot_fraction=hot_fraction,
                    event_bytes=event_bytes,
                    stats=stats,
                    stop=stop_gen,
                )
            else:
                await run_uncooked(
                    host=str(settings.get("uncooked_host", "127.0.0.1")),
                    port=int(settings.get("uncooked_port", 39997)),
                    eps=eps,
                    duration_s=duration_s,
                    connections=connections,
                    hot_fraction=hot_fraction,
                    event_bytes=event_bytes,
                    stats=stats,
                    stop=stop_gen,
                )
        finally:
            stop_gen.set()
            stop_obs.set()
            await asyncio.gather(obs_task, return_exceptions=True)

    # Brief settle so observer/ES can catch up before summary.
    await asyncio.sleep(min(5.0, interval_s))
    snap = stats.snapshot()
    summary = summarize(
        metrics_csv=metrics_csv,
        gen_snapshot=snap,
        scenario_id=scenario_id,
        phase=phase,
    )
    write_summary(out_dir / f"summary_{phase}.json", summary)
    logger.info(
        "phase=%s done sent=%s avg_eps=%.0f gbps=%.4f passed=%s notes=%s",
        phase,
        summary["sent_events"],
        summary["avg_eps"],
        summary["avg_gbps_payload"],
        summary["passed"],
        summary.get("notes"),
    )
    return summary


async def async_main(args: argparse.Namespace) -> int:
    cfg = _load_config(Path(args.scenarios))
    settings = _merge_settings(cfg, args.scenario, args)

    run_id = args.run_id or time.strftime("%Y%m%d-%H%M%S")
    out_dir = Path(args.results) / f"{args.scenario}_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "settings.yaml").write_text(
        yaml.safe_dump({**settings, "scenario_id": args.scenario}, sort_keys=False),
        encoding="utf-8",
    )

    target_eps = float(settings.get("target_eps", 5000))
    steady_s = float(
        settings.get("duration")
        if settings.get("duration") is not None
        else args.duration
        if args.duration is not None
        else 120
    )
    warm_eps = float(settings.get("warm_eps", 500))
    warm_s = float(settings.get("warm_seconds", 60))
    burst_mult = float(settings.get("burst_multiplier", 2.0))
    burst_s = float(settings.get("burst_seconds", 45))

    summaries: list[dict[str, Any]] = []

    if not args.steady_only:
        summaries.append(
            await _run_phase(
                settings=settings,
                eps=warm_eps,
                duration_s=warm_s,
                phase="warm",
                out_dir=out_dir,
                scenario_id=args.scenario,
            )
        )

    summaries.append(
        await _run_phase(
            settings=settings,
            eps=target_eps,
            duration_s=steady_s,
            phase="steady",
            out_dir=out_dir,
            scenario_id=args.scenario,
        )
    )

    if not args.steady_only and burst_s > 0:
        summaries.append(
            await _run_phase(
                settings=settings,
                eps=target_eps * burst_mult,
                duration_s=burst_s,
                phase="burst",
                out_dir=out_dir,
                scenario_id=args.scenario,
            )
        )

    overall = {
        "scenario": args.scenario,
        "run_id": run_id,
        "out_dir": str(out_dir),
        "phases": summaries,
        "steady_passed": next(
            (s.get("passed") for s in summaries if s.get("phase") == "steady"),
            False,
        ),
    }
    write_summary(out_dir / "summary.json", overall)
    logger.info("results written to %s", out_dir)
    return 0 if overall["steady_passed"] else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loadtest", description="Splash load-test harness")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run a named scenario")
    run.add_argument("--scenario", "-s", required=True, help="Scenario id (S1..S4)")
    run.add_argument(
        "--scenarios",
        default=str(DEFAULT_SCENARIOS),
        help="Path to scenarios.yaml",
    )
    run.add_argument("--eps", type=float, default=None, help="Override target eps")
    run.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Steady-phase duration seconds (default 120)",
    )
    run.add_argument("--connections", type=int, default=None)
    run.add_argument("--event-bytes", type=int, default=None)
    run.add_argument("--namespace", default=None, help="DATA_STREAM_NAMESPACE for ES count")
    run.add_argument("--elastic-host", default=None)
    run.add_argument("--elastic-api-key", default=None)
    run.add_argument("--results", default=str(DEFAULT_RESULTS))
    run.add_argument("--run-id", default=None)
    run.add_argument("--skip-warm", action="store_true")
    run.add_argument("--skip-burst", action="store_true")
    run.add_argument(
        "--steady-only",
        action="store_true",
        help="Only run the steady phase (implies skip warm/burst)",
    )
    run.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.cmd == "run":
        if args.steady_only:
            args.skip_warm = True
            args.skip_burst = True
        return asyncio.run(async_main(args))
    parser.error(f"unknown command {args.cmd}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
