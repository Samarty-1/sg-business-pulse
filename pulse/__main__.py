"""python -m pulse run [--skip-extract]

Runs the whole Dagster job in-process (no UI needed) and exits non-zero if any
step or blocking contract fails.
"""
from __future__ import annotations

import argparse
import logging
import sys

import dagster as dg

from .definitions import ALL_ASSETS, ALL_CHECKS, Paths


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m pulse")
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="extract, check, load, build history and marts")
    run.add_argument("--skip-extract", action="store_true", help="use files already landed")
    run.add_argument("--root", default="data")
    run.add_argument("--state-dir", default="state")
    run.add_argument("--results-dir", default="results")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pulse").setLevel(logging.INFO)
    selection = dg.AssetSelection.all()
    if args.skip_extract:
        selection = selection - dg.AssetSelection.groups("extract")
    result = dg.materialize(
        assets=[*ALL_ASSETS, *ALL_CHECKS],
        selection=selection,
        resources={"paths": Paths(root=args.root, state_dir=args.state_dir, results_dir=args.results_dir)},
        run_config={"loggers": {"console": {"config": {"log_level": "WARNING"}}}},
        raise_on_error=False,
    )
    failed_checks = []
    for ev in result.get_asset_check_evaluations():
        detail = ev.metadata.get("detail")
        detail = detail.value if detail is not None else ""
        label = "ok  " if ev.passed else ("FAIL" if ev.severity == dg.AssetCheckSeverity.ERROR else "warn")
        print(f"[{label}] {ev.check_name}: {detail}")
        if not ev.passed and ev.severity == dg.AssetCheckSeverity.ERROR:
            failed_checks.append(ev.check_name)
    if failed_checks:
        print(f"Blocking contracts failed: {failed_checks}. Downstream tables were not rebuilt.",
              file=sys.stderr)
    return 0 if result.success and not failed_checks else 1


if __name__ == "__main__":
    sys.exit(main())
