"""The whole Dagster job, end to end, on fixture releases."""
from __future__ import annotations

import csv

import duckdb

from pulse.__main__ import main
from tests.fixtures import R1, R2, write_release, write_singstat


def run(tmp_path):
    return main(["run", "--skip-extract", "--root", str(tmp_path / "data"),
                 "--state-dir", str(tmp_path / "state"), "--results-dir", str(tmp_path / "results")])


def rows(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_two_monthly_runs_build_history_and_marts(tmp_path):
    data = tmp_path / "data"
    write_singstat(data)
    write_release(data, "2026-09-14", R1)
    assert run(tmp_path) == 0
    write_release(data, "2026-10-14", R2)
    assert run(tmp_path) == 0

    changes = rows(tmp_path / "results" / "mart_status_changes.csv")
    oct_changes = {(r["change_type"], r["from_group"], r["to_group"]): int(r["entities"])
                   for r in changes if r["release_date"] == "2026-10-14"}
    assert oct_changes == {("changed", "live", "closed"): 1, ("new", "", "live"): 1,
                           ("removed", "live", "removed"): 1}

    # the warehouse file is disposable: delete it, rerun, and the history survives via state/
    (data / "warehouse.duckdb").unlink()
    assert run(tmp_path) == 0
    con = duckdb.connect(str(data / "warehouse.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM history_runs").fetchone()[0] == 2
    con.close()

    sales = rows(tmp_path / "results" / "mart_sales_indices.csv")
    yoy = {r["month"]: r["yoy_change"] for r in sales}
    assert float(yoy["2025-01-01"]) == round(93 / 90 - 1, 4)
    assert yoy["2024-01-01"] == ""  # no month a year earlier


def test_a_broken_release_stops_before_history_and_marts(tmp_path):
    data = tmp_path / "data"
    write_singstat(data)
    write_release(data, "2026-09-14", R1)
    assert run(tmp_path) == 0
    before = (tmp_path / "state" / "entity_history.parquet").read_bytes()

    write_release(data, "2026-10-14", R2 + [R2[0]])  # duplicated UEN
    assert run(tmp_path) == 1
    assert (tmp_path / "state" / "entity_history.parquet").read_bytes() == before
