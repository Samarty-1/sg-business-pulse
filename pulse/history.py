"""Type 2 history of every entity, built from full monthly snapshots.

ACRA republishes the whole register each month and says nothing about what changed.
Comparing each release with the open version of every entity recovers the changes:

    new         UEN not seen before
    changed     a tracked field differs (name, status, industry code, postcode)
    removed     UEN no longer in the register
    reappeared  a removed UEN is back

Each change closes the open version (valid_to = release date) and opens a new one.
Processing a release is idempotent (the same release twice is a no-op) and refuses
releases older than the latest one applied, which would corrupt the timeline.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb

# Source fields only. Derived fields (status group, industry) are applied on read, so
# changing a business rule never shows up as a fake change in the history.
TRACKED = ["entity_name", "status", "ssic_code_raw", "postal_code"]

HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS entity_history (
    uen           VARCHAR NOT NULL,
    entity_name   VARCHAR,
    status        VARCHAR,
    ssic_code_raw VARCHAR,
    postal_code   VARCHAR,
    row_hash      VARCHAR NOT NULL,
    valid_from    DATE    NOT NULL,
    valid_to      DATE,               -- NULL while this is the current version
    change_type   VARCHAR NOT NULL,   -- initial | new | changed | removed | reappeared
    is_removed    BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS history_runs (
    snapshot_date DATE PRIMARY KEY,
    applied_at    TIMESTAMP NOT NULL,
    n_rows        BIGINT NOT NULL,
    n_new         BIGINT NOT NULL,
    n_changed     BIGINT NOT NULL,
    n_removed     BIGINT NOT NULL,
    n_reappeared  BIGINT NOT NULL,
    n_unchanged   BIGINT NOT NULL
);
"""

_HASH = "md5(concat_ws('|', " + ", ".join(f"coalesce({c}, '<null>')" for c in TRACKED) + "))"


class OutOfOrderRelease(RuntimeError):
    pass


@dataclass(frozen=True)
class RunSummary:
    snapshot_date: date
    applied: bool
    n_new: int = 0
    n_changed: int = 0
    n_removed: int = 0
    n_reappeared: int = 0
    n_unchanged: int = 0


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(HISTORY_DDL)


def apply_release(con: duckdb.DuckDBPyConnection, source: str = "stg_entities") -> RunSummary:
    """Fold the release in `source` (one row per UEN, with snapshot_date) into entity_history."""
    ensure_tables(con)
    dates = con.execute(f"SELECT DISTINCT snapshot_date FROM {source}").fetchall()
    if len(dates) != 1:
        raise ValueError(f"{source} must hold exactly one snapshot, found {len(dates)}")
    snap = dates[0][0]
    if isinstance(snap, str):
        snap = date.fromisoformat(snap)

    latest = con.execute("SELECT max(snapshot_date) FROM history_runs").fetchone()[0]
    if latest is not None and snap == latest:
        return RunSummary(snap, applied=False)
    if latest is not None and snap < latest:
        raise OutOfOrderRelease(f"release {snap} is older than the latest applied ({latest})")
    first_run = latest is None

    cols = ", ".join(TRACKED)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _incoming AS
            SELECT uen, {cols}, {_HASH} AS row_hash FROM {source}
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _diff AS
            SELECT
                coalesce(i.uen, h.uen) AS uen,
                CASE
                    WHEN h.uen IS NULL                         THEN 'new'
                    WHEN i.uen IS NULL AND NOT h.is_removed    THEN 'removed'
                    WHEN i.uen IS NULL                         THEN 'still_removed'
                    WHEN h.is_removed                          THEN 'reappeared'
                    WHEN i.row_hash <> h.row_hash              THEN 'changed'
                    ELSE 'unchanged'
                END AS diff
            FROM _incoming i
            FULL OUTER JOIN (SELECT * FROM entity_history WHERE valid_to IS NULL) h USING (uen)
        """)
        # Close the open version of everything that changed, left, or came back.
        con.execute("""
            UPDATE entity_history SET valid_to = ?
            WHERE valid_to IS NULL
              AND uen IN (SELECT uen FROM _diff WHERE diff IN ('changed', 'removed', 'reappeared'))
        """, [snap])
        # Open new versions for arrivals and changes.
        con.execute(f"""
            INSERT INTO entity_history
            SELECT i.uen, {", ".join("i." + c for c in TRACKED)}, i.row_hash, ?, NULL,
                   CASE WHEN d.diff = 'new' AND ? THEN 'initial' ELSE d.diff END, FALSE
            FROM _incoming i JOIN _diff d USING (uen)
            WHERE d.diff IN ('new', 'changed', 'reappeared')
        """, [snap, first_run])
        # A removal is a tombstone version: the last known values, flagged as removed.
        con.execute(f"""
            INSERT INTO entity_history
            SELECT h.uen, {", ".join("h." + c for c in TRACKED)}, h.row_hash, ?, NULL, 'removed', TRUE
            FROM entity_history h JOIN _diff d USING (uen)
            WHERE d.diff = 'removed' AND h.valid_to = ? AND NOT h.is_removed
        """, [snap, snap])

        counts = dict(con.execute("SELECT diff, count(*) FROM _diff GROUP BY diff").fetchall())
        n_rows = con.execute("SELECT count(*) FROM _incoming").fetchone()[0]
        summary = RunSummary(
            snap, True,
            n_new=counts.get("new", 0), n_changed=counts.get("changed", 0),
            n_removed=counts.get("removed", 0), n_reappeared=counts.get("reappeared", 0),
            n_unchanged=counts.get("unchanged", 0),
        )
        con.execute(
            "INSERT INTO history_runs VALUES (?, current_timestamp, ?, ?, ?, ?, ?, ?)",
            [snap, n_rows, summary.n_new, summary.n_changed, summary.n_removed,
             summary.n_reappeared, summary.n_unchanged],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return summary


def as_of(con: duckdb.DuckDBPyConnection, when: date) -> duckdb.DuckDBPyRelation:
    """The register as it stood on a given date (entities not removed by then)."""
    return con.sql(f"""
        SELECT * EXCLUDE (row_hash) FROM entity_history
        WHERE valid_from <= DATE '{when.isoformat()}'
          AND (valid_to IS NULL OR valid_to > DATE '{when.isoformat()}')
          AND NOT is_removed
    """)
