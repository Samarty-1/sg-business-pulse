"""Dagster definitions: the pipeline as assets, contracts as blocking asset checks,
and a monthly schedule.

    dagster dev -m pulse.definitions        # UI at http://localhost:3000
    python -m pulse run                     # same pipeline, no UI
"""
from __future__ import annotations

from pathlib import Path

import dagster as dg

from . import contracts, history, warehouse
from .extract import Manifest, land_acra, land_singstat


class Paths(dg.ConfigurableResource):
    root: str = "data"
    state_dir: str = "state"
    results_dir: str = "results"

    @property
    def db(self) -> Path:
        return Path(self.root) / "warehouse.duckdb"

    def manifest(self) -> Manifest:
        return Manifest(Path(self.root) / "manifest.json")

    def connect(self):
        return warehouse.connect(self.db)


# ------------------------------------------------------------------ extract

@dg.asset(group_name="extract", description="SingStat retail and F&B sales indices (JSON), landed if changed.")
def singstat_files(paths: Paths) -> dg.MaterializeResult:
    landed = land_singstat(Path(paths.root), paths.manifest())
    return dg.MaterializeResult(metadata={
        "changed": sum(x.changed for x in landed),
        "snapshots": dg.MetadataValue.json({x.name: x.snapshot_date for x in landed}),
    })


@dg.asset(group_name="extract", description="ACRA register: 27 CSV files, downloaded only when republished.")
def acra_files(paths: Paths) -> dg.MaterializeResult:
    landed = land_acra(Path(paths.root), paths.manifest())
    return dg.MaterializeResult(metadata={
        "files": len(landed),
        "downloaded": sum(x.changed for x in landed),
        "snapshot_dates": dg.MetadataValue.json(sorted({x.snapshot_date for x in landed})),
    })


# ------------------------------------------------------------------ load + contracts

@dg.asset(deps=[acra_files], group_name="warehouse", description="Latest ACRA release, all columns as text.")
def raw_acra(paths: Paths) -> dg.MaterializeResult:
    con = paths.connect()
    try:
        warehouse.load_raw_acra(con, Path(paths.root), paths.manifest())
        n = con.execute("SELECT count(*) FROM raw_acra").fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"rows": n})


def _previous_rows(con) -> int | None:
    history.ensure_tables(con)
    row = con.execute("SELECT n_rows FROM history_runs ORDER BY snapshot_date DESC LIMIT 1").fetchone()
    return row[0] if row else None


@dg.multi_asset_check(
    specs=[dg.AssetCheckSpec(name, asset=raw_acra, blocking=blocking) for name, blocking in [
        ("required_columns", True), ("release_complete", True), ("uen_unique_and_present", True),
        ("registration_dates_parse", True), ("row_count_drift", True), ("known_statuses", False),
    ]],
)
def raw_acra_contracts(paths: Paths):
    con = paths.connect()
    try:
        warehouse.restore_state(con, Path(paths.state_dir))
        results = contracts.run_release_checks(con, _previous_rows(con))
    finally:
        con.close()
    for r in results:
        yield dg.AssetCheckResult(
            asset_key=raw_acra.key, check_name=r.name, passed=r.passed,
            severity=dg.AssetCheckSeverity.ERROR if r.blocking else dg.AssetCheckSeverity.WARN,
            metadata={"detail": r.detail},
        )


@dg.asset(deps=[raw_acra], group_name="warehouse", description="Typed, cleaned entities; statuses grouped.")
def stg_entities(paths: Paths) -> dg.MaterializeResult:
    con = paths.connect()
    try:
        warehouse.load_seeds(con)
        warehouse.run_sql(con, "stg_entities.sql")
        groups = dict(con.execute("SELECT status_group, count(*) FROM stg_entities GROUP BY 1").fetchall())
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"rows": sum(groups.values()), "by_status": dg.MetadataValue.json(groups)})


@dg.multi_asset_check(
    specs=[dg.AssetCheckSpec("status_grouping_complete", asset=stg_entities, blocking=True),
           dg.AssetCheckSpec("industry_mostly_classified", asset=stg_entities, blocking=False)],
)
def stg_entities_contracts(paths: Paths):
    con = paths.connect()
    try:
        results = warehouse.staging_checks(con)
    finally:
        con.close()
    for r in results:
        yield dg.AssetCheckResult(
            asset_key=stg_entities.key, check_name=r.name, passed=r.passed,
            severity=dg.AssetCheckSeverity.ERROR if r.blocking else dg.AssetCheckSeverity.WARN,
            metadata={"detail": r.detail},
        )


@dg.asset(deps=[stg_entities], group_name="warehouse",
          description="SCD2 history of every entity: one version per change between releases.")
def entity_history(paths: Paths) -> dg.MaterializeResult:
    con = paths.connect()
    try:
        warehouse.restore_state(con, Path(paths.state_dir))
        summary = history.apply_release(con)
        warehouse.save_state(con, Path(paths.state_dir))
        versions = con.execute("SELECT count(*) FROM entity_history").fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={
        "release": str(summary.snapshot_date), "applied": summary.applied, "versions": versions,
        "new": summary.n_new, "changed": summary.n_changed, "removed": summary.n_removed,
        "reappeared": summary.n_reappeared,
    })


@dg.asset(deps=[singstat_files], group_name="warehouse", description="Sales indices, one row per series and month.")
def stg_singstat(paths: Paths) -> dg.MaterializeResult:
    con = paths.connect()
    try:
        warehouse.load_singstat(con, warehouse.latest_files(Path(paths.root), paths.manifest(), "singstat"))
        n = con.execute("SELECT count(*) FROM stg_singstat").fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"rows": n})


@dg.asset(deps=[entity_history, stg_singstat], group_name="marts",
          description="Business-facing tables, also exported to results/*.csv.")
def marts(paths: Paths) -> dg.MaterializeResult:
    con = paths.connect()
    try:
        warehouse.run_sql(con, "marts.sql")
        warehouse.export_marts(con, Path(paths.results_dir))
        sizes = {m: con.execute(f"SELECT count(*) FROM {m}").fetchone()[0] for m in warehouse.MARTS}
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.json(sizes)})


ALL_ASSETS = [singstat_files, acra_files, raw_acra, stg_entities, entity_history, stg_singstat, marts]
ALL_CHECKS = [raw_acra_contracts, stg_entities_contracts]

refresh_job = dg.define_asset_job("monthly_refresh", selection=dg.AssetSelection.all(),
                                  executor_def=dg.in_process_executor)

defs = dg.Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[refresh_job],
    # ACRA republishes mid-month; run on the 20th so a late release is still caught.
    schedules=[dg.ScheduleDefinition(job=refresh_job, cron_schedule="0 6 20 * *",
                                     execution_timezone="Asia/Singapore")],
    resources={"paths": Paths()},
)
