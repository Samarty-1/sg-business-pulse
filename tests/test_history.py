"""The SCD2 history: what a sequence of full snapshots turns into."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pulse import history, warehouse
from pulse.extract import Manifest
from tests.fixtures import R1, R2, R3, write_release


def stage(con, root: Path) -> None:
    warehouse.load_seeds(con)
    warehouse.load_raw_acra(con, root, Manifest(root / "manifest.json"))
    warehouse.run_sql(con, "stg_entities.sql")


def apply(con, root: Path, snapshot: str, rows: list[dict]) -> history.RunSummary:
    write_release(root, snapshot, rows)
    stage(con, root)
    return history.apply_release(con)


@pytest.fixture
def con():
    c = warehouse.connect()
    yield c
    c.close()


def versions(con, uen):
    return con.execute(
        "SELECT entity_name, status, valid_from, valid_to, change_type, is_removed "
        "FROM entity_history WHERE uen = ? ORDER BY valid_from, is_removed", [uen]).fetchall()


def test_first_release_is_all_initial(con, tmp_path):
    s = apply(con, tmp_path, "2026-09-14", R1)
    assert (s.applied, s.n_new, s.n_changed, s.n_removed) == (True, 4, 0, 0)
    types = {r[0] for r in con.execute("SELECT change_type FROM entity_history").fetchall()}
    assert types == {"initial"}


def test_second_release_records_each_kind_of_change(con, tmp_path):
    apply(con, tmp_path, "2026-09-14", R1)
    s = apply(con, tmp_path, "2026-10-14", R2)
    assert (s.n_new, s.n_changed, s.n_removed, s.n_unchanged) == (1, 2, 1, 1)

    d1, d2 = date(2026, 9, 14), date(2026, 10, 14)
    # struck off: old version closed on the release date, new version open
    assert versions(con, "201900001A") == [
        ("KOPI CORNER PTE. LTD.", "Live Company", d1, d2, "initial", False),
        ("KOPI CORNER PTE. LTD.", "Struck Off", d2, None, "changed", False),
    ]
    # renamed
    assert [v[0] for v in versions(con, "201900002B")] == ["LAKSA HOUSE PTE. LTD.", "LAKSA HOUSE SINGAPORE PTE. LTD."]
    # untouched: still one open version from the first release
    assert versions(con, "53000003C") == [("TAN HARDWARE", "Live", d1, None, "initial", False)]
    # removed: closed, plus an open tombstone carrying the last known values
    assert versions(con, "201900004D") == [
        ("BYTE WORKS PTE. LTD.", "Live Company", d1, d2, "initial", False),
        ("BYTE WORKS PTE. LTD.", "Live Company", d2, None, "removed", True),
    ]
    # new
    assert versions(con, "202600005E")[0][4] == "new"


def test_exactly_one_open_version_per_entity(con, tmp_path):
    for snap, rows in [("2026-08-14", R1), ("2026-09-14", R2), ("2026-10-14", R3)]:
        apply(con, tmp_path, snap, rows)
    bad = con.execute(
        "SELECT uen FROM entity_history WHERE valid_to IS NULL GROUP BY uen HAVING count(*) <> 1").fetchall()
    assert bad == []
    overlaps = con.execute("""
        SELECT count(*) FROM entity_history a JOIN entity_history b
          ON a.uen = b.uen AND a.valid_from < b.valid_from
         AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
    """).fetchone()[0]
    assert overlaps == 0


def test_removed_entity_can_reappear(con, tmp_path):
    apply(con, tmp_path, "2026-08-14", R1)
    apply(con, tmp_path, "2026-09-14", R2)
    s = apply(con, tmp_path, "2026-10-14", R3)
    assert s.n_reappeared == 1
    assert [v[4] for v in versions(con, "201900004D")] == ["initial", "removed", "reappeared"]


def test_same_release_twice_is_a_no_op(con, tmp_path):
    apply(con, tmp_path, "2026-09-14", R1)
    before = con.execute("SELECT count(*) FROM entity_history").fetchone()[0]
    s = history.apply_release(con)
    assert not s.applied
    assert con.execute("SELECT count(*) FROM entity_history").fetchone()[0] == before
    assert con.execute("SELECT count(*) FROM history_runs").fetchone()[0] == 1


def test_older_release_is_refused(con, tmp_path):
    apply(con, tmp_path, "2026-10-14", R2)
    with pytest.raises(history.OutOfOrderRelease):
        apply(con, tmp_path, "2026-09-14", R1)


def test_failed_apply_leaves_history_untouched(con, tmp_path):
    apply(con, tmp_path, "2026-09-14", R1)
    before = con.execute("SELECT * FROM entity_history ORDER BY ALL").fetchall()
    write_release(tmp_path, "2026-10-14", R2)
    stage(con, tmp_path)
    con.execute("DROP TABLE history_runs")  # the final INSERT will fail mid-transaction
    con.execute("CREATE TABLE history_runs (snapshot_date DATE)")  # wrong shape on purpose
    with pytest.raises(Exception):
        history.apply_release(con)
    assert con.execute("SELECT * FROM entity_history ORDER BY ALL").fetchall() == before


def test_as_of_reconstructs_the_register_between_releases(con, tmp_path):
    apply(con, tmp_path, "2026-09-14", R1)
    apply(con, tmp_path, "2026-10-14", R2)
    sept = dict(history.as_of(con, date(2026, 9, 30)).select("uen, status").fetchall())
    octo = dict(history.as_of(con, date(2026, 10, 20)).select("uen, status").fetchall())
    assert sept["201900001A"] == "Live Company" and "202600005E" not in sept and "201900004D" in sept
    assert octo["201900001A"] == "Struck Off" and "202600005E" in octo and "201900004D" not in octo


def test_rule_changes_do_not_create_fake_history(con, tmp_path):
    """Only source fields are hashed, so re-deriving status groups changes nothing."""
    apply(con, tmp_path, "2026-09-14", R1)
    tracked = set(history.TRACKED)
    assert "status_group" not in tracked and "industry" not in tracked
