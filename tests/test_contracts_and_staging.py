"""Release contracts and the staging rules."""
from __future__ import annotations

import pytest

from pulse import contracts, warehouse
from pulse.extract import Manifest
from tests.fixtures import R1, entity, write_release


@pytest.fixture
def con():
    c = warehouse.connect()
    yield c
    c.close()


def load(con, root, rows, snapshot="2026-10-14", files=2):
    write_release(root, snapshot, rows, files=files)
    warehouse.load_raw_acra(con, root, Manifest(root / "manifest.json"))


def by_name(results):
    return {r.name: r for r in results}


def test_clean_release_passes_every_blocking_check(con, tmp_path):
    load(con, tmp_path, R1)
    results = contracts.run_release_checks(con, previous_rows=None)
    assert all(r.passed for r in results if r.blocking)
    contracts.enforce(results)


def test_duplicate_uen_is_blocking(con, tmp_path):
    load(con, tmp_path, R1 + [R1[0]])
    r = by_name(contracts.run_release_checks(con, None))["uen_unique_and_present"]
    assert not r.passed and r.blocking
    with pytest.raises(contracts.ContractViolation):
        contracts.enforce(contracts.run_release_checks(con, None))


def test_missing_column_is_blocking(con, tmp_path):
    load(con, tmp_path, R1)
    con.execute("ALTER TABLE raw_acra DROP COLUMN primary_ssic_code")
    assert not contracts.required_columns(con).passed


def test_mixed_snapshot_release_is_blocking(con, tmp_path):
    write_release(tmp_path, "2026-09-14", R1, files=2)
    # only one of the two files is republished: a half-finished release
    write_release(tmp_path, "2026-10-14", R1[:2], files=1)
    warehouse.load_raw_acra(con, tmp_path, Manifest(tmp_path / "manifest.json"))
    assert not contracts.release_complete(con).passed


def test_unchanged_file_carries_forward_into_the_new_release(con, tmp_path):
    """A file republished with identical bytes stays in last month's folder but belongs
    to this month's release, so the release is complete and dated this month."""
    write_release(tmp_path, "2026-09-14", R1, files=2)
    write_release(tmp_path, "2026-10-14", R1[0::2], files=1)  # d_test0 has new content
    m = Manifest(tmp_path / "manifest.json")
    m.entries["d_test1"]["published"] = "2026-10-14T17:00:00+08:00"  # republished, same bytes
    m.save()
    warehouse.load_raw_acra(con, tmp_path, Manifest(tmp_path / "manifest.json"))
    assert contracts.release_complete(con).passed
    assert {str(r[0]) for r in con.execute("SELECT DISTINCT snapshot_date FROM raw_acra").fetchall()} == {"2026-10-14"}
    assert con.execute("SELECT count(*) FROM raw_acra").fetchone()[0] == len(R1)


@pytest.mark.parametrize("previous,current,ok", [
    (None, 100, True), (1000, 1004, True), (1000, 970, False), (1000, 1100, False),
])
def test_row_count_drift(previous, current, ok):
    assert contracts.row_count_drift(current, previous).passed is ok


def test_unknown_status_warns_but_does_not_block(con, tmp_path):
    load(con, tmp_path, R1 + [entity("209900009Z", "NEW STATUS PTE. LTD.", status="Placed Under Review")])
    r = by_name(contracts.run_release_checks(con, None))["known_statuses"]
    assert not r.passed and not r.blocking


def test_every_known_status_has_a_group(con):
    warehouse.run_sql(con, "macros.sql")
    for status in contracts.KNOWN_STATUSES:
        assert con.execute("SELECT status_group(?)", [status]).fetchone()[0] != "unknown", status


@pytest.mark.parametrize("status,group", [
    ("Live Company", "live"), ("Live", "live"),
    ("Live (Receiver or Receiver and Manager appointed)", "closing"),
    ("Gazetted To Be Struck Off", "closing"), ("Struck Off", "closed"),
    ("Cancelled (Non-Renewal)", "closed"), ("Converted To LLP", "converted"),
    ("Ceased Registration (Transferred to Singapore)", "converted"), ("Something New", "unknown"),
])
def test_status_groups(con, status, group):
    warehouse.run_sql(con, "macros.sql")
    assert con.execute("SELECT status_group(?)", [status]).fetchone()[0] == group


def test_staging_cleans_postcodes_and_industries(con, tmp_path):
    rows = [
        entity("1", "OLD POSTCODE", postal="0718"),
        entity("2", "BAD SECTOR", postal="912345"),
        entity("3", "GOOD", postal="238801"),
        entity("4", "LEGACY SSIC", ssic="5611"),
        entity("5", "NO SSIC", ssic="na"),
    ]
    load(con, tmp_path, rows, files=1)
    warehouse.load_seeds(con)
    warehouse.run_sql(con, "stg_entities.sql")
    got = {r[0]: r[1:] for r in con.execute(
        "SELECT uen, postal_code_valid, postal_sector, industry, is_food_and_beverage FROM stg_entities").fetchall()}
    assert got["1"][:2] == (False, None)
    assert got["2"][:2] == (False, None)
    assert got["3"][:2] == (True, "23")
    assert got["3"][2:] == ("Accommodation and food services", True)
    assert got["4"][2:] == ("Unclassified", False)  # legacy 4-digit code is not padded into F&B
    assert got["5"][2:] == ("Unclassified", False)
    assert warehouse.staging_checks(con)[0].passed
