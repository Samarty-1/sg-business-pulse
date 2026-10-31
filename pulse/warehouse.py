"""Build the DuckDB warehouse from landed files.

    raw_acra  ->  [release contracts]  ->  stg_entities  ->  entity_history (SCD2)  ->  marts
    singstat  ->  stg_singstat  ----------------------------------------------------->  marts

entity_history is the only table that carries state from one run to the next. It is
persisted as Parquet under state/, so the warehouse file itself can be deleted and
rebuilt at any time.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb

from . import contracts, history
from .extract import Manifest

log = logging.getLogger(__name__)

PKG = Path(__file__).resolve().parent
SQL = PKG / "sql"
SEEDS = PKG / "seeds"

MARTS = ["mart_formations_monthly", "mart_cohort_survival", "mart_live_by_sector",
         "mart_status_changes", "mart_sales_indices"]


def connect(db_path: Path | str = ":memory:") -> duckdb.DuckDBPyConnection:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _sql_list(paths: list[Path]) -> str:
    return "[" + ", ".join("'" + p.as_posix().replace("'", "''") + "'" for p in paths) + "]"


def latest_files(root: Path, manifest: Manifest, source: str) -> list[Path]:
    out = []
    for entry in manifest.entries.values():
        entry_source = entry.get("source") or entry["path"].split("/")[1]
        if entry_source == source:
            out.append(root / entry["path"])
    return sorted(out)


def load_seeds(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE ssic_sections AS
        SELECT * FROM read_csv('{(SEEDS / "ssic_sections.csv").as_posix()}', all_varchar = true, header = true)
    """)


def load_raw_acra(con: duckdb.DuckDBPyConnection, root: Path, manifest: Manifest) -> None:
    """Load the current release: the latest landed copy of each of ACRA's files.

    A file whose bytes did not change this month still sits under last month's folder,
    but it was *published* in this month's release. So the release date is taken from
    the publish dates in the manifest, and `release_files` keeps them for the
    release_complete contract.
    """
    entries = [e for e in manifest.entries.values()
               if (e.get("source") or e["path"].split("/")[1]) == "acra"]
    if not entries:
        raise FileNotFoundError("no ACRA files landed; run the extract step first")
    con.execute("CREATE OR REPLACE TABLE release_files (path VARCHAR, published_date DATE)")
    con.executemany("INSERT INTO release_files VALUES (?, ?)",
                    [((root / e["path"]).as_posix(), e["published"][:10]) for e in entries])
    files = sorted(root / e["path"] for e in entries)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_acra AS
        SELECT r.* EXCLUDE (snapshot, filename),
               (SELECT max(published_date) FROM release_files) AS snapshot_date
        FROM read_csv({_sql_list(files)}, all_varchar = true, header = true,
                      union_by_name = true, hive_partitioning = true, filename = true) r
    """)


def load_singstat(con: duckdb.DuckDBPyConnection, files: list[Path]) -> None:
    rows = []
    for f in files:
        table = json.loads(f.read_text(encoding="utf-8"))
        name = {"M602121": "retail_sales_index", "M602131": "fnb_services_index"}.get(table["id"], table["id"])
        for series in table["row"]:
            for point in series["columns"]:
                value = point["value"]
                if value in ("", "na", "-", None):
                    continue
                month = datetime.strptime(point["key"], "%Y %b").date()
                rows.append((name, series["rowText"].strip(), month, float(value)))
    con.execute("CREATE OR REPLACE TABLE stg_singstat (table_name VARCHAR, series VARCHAR, month DATE, value DOUBLE)")
    if rows:
        con.executemany("INSERT INTO stg_singstat VALUES (?, ?, ?, ?)", rows)
    dupes = con.execute(
        "SELECT count(*) FROM (SELECT 1 FROM stg_singstat GROUP BY table_name, series, month HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        raise contracts.ContractViolation(f"stg_singstat: {dupes} duplicated (table, series, month) keys")


def run_sql(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Run one SQL model, always with the shared business-rule macros defined first."""
    con.execute((SQL / "macros.sql").read_text(encoding="utf-8"))
    con.execute((SQL / name).read_text(encoding="utf-8"))


def staging_checks(con: duckdb.DuckDBPyConnection) -> list[contracts.CheckResult]:
    total, unknown, unclassified = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE coalesce(status_group, 'unknown') = 'unknown'),
               count(*) FILTER (WHERE industry = 'Unclassified')
        FROM stg_entities
    """).fetchone()
    unknown_share = unknown / total if total else 0
    unknown_labels = [r[0] for r in con.execute(
        "SELECT DISTINCT status FROM stg_entities WHERE coalesce(status_group, 'unknown') = 'unknown' "
        "ORDER BY 1").fetchall()]
    return [
        contracts.CheckResult(
            "status_grouping_complete", unknown_share <= 0.001,
            f"{unknown:,} rows ({unknown_share:.3%}) have a status no rule covers: {unknown_labels}"),
        contracts.CheckResult(
            "industry_mostly_classified", unclassified / total <= 0.02 if total else False,
            f"{unclassified:,} rows ({unclassified / total if total else 0:.2%}) have no valid SSIC code",
            blocking=False),
    ]


def restore_state(con: duckdb.DuckDBPyConnection, state_dir: Path) -> None:
    history.ensure_tables(con)
    for table in ("entity_history", "history_runs"):
        f = state_dir / f"{table}.parquet"
        if f.exists() and con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0:
            con.execute(f"INSERT INTO {table} SELECT * FROM read_parquet('{f.as_posix()}')")


def save_state(con: duckdb.DuckDBPyConnection, state_dir: Path) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    for table in ("entity_history", "history_runs"):
        tmp = state_dir / f"{table}.parquet.tmp"
        con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO '{tmp.as_posix()}' "
                    "(FORMAT parquet, COMPRESSION zstd)")
        tmp.replace(state_dir / f"{table}.parquet")


def export_marts(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for mart in MARTS:
        con.execute(f"COPY {mart} TO '{(out_dir / f'{mart}.csv').as_posix()}' (HEADER)")
    release = con.execute("SELECT max(snapshot_date) FROM history_runs").fetchone()[0]
    (out_dir / "release.txt").write_text(f"{release}\n", encoding="utf-8")
