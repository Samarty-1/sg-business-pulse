"""Data contracts: checks a release must pass before anything downstream is rebuilt.

Each check returns a CheckResult. `blocking` checks stop the pipeline; the others are
reported as warnings. The same functions back the Dagster asset checks and the tests.
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .sources import ACRA_REQUIRED_COLUMNS

# Every status label in the 2026-10-14 release. A new label is not an error in itself,
# but it is surfaced, so that a person decides which group it belongs to.
KNOWN_STATUSES = {
    "Amalgamated", "Cancellation In Progress", "Cancelled", "Cancelled (Non-Renewal)",
    "Ceased Registration", "Ceased Registration (Transferred to Singapore)", "Converted To LLP",
    "Dissolved", "Dissolved - Compulsory Winding Up", "Dissolved - Compulsory Winding Up (Insolvency)",
    "Dissolved - Creditors Voluntary Winding Up", "Dissolved - Members Voluntary Winding Up",
    "Dissolved - Partner Voluntary Winding Up",
    "Dissolved - Pursuant to Section 212(1)(D) Of The Companies Act", "Dissolved - Simplified Winding Up",
    "Gazetted To Be Struck Off", "In Liquidation", "In Liquidation - Compulsory Winding Up",
    "In Liquidation - Compulsory Winding Up (Insolvency)", "In Liquidation - Creditors voluntary winding up",
    "In Liquidation - Members voluntary winding up", "In Liquidation - Partners voluntary winding up",
    "In Liquidation - Simplified Winding Up", "Live", "Live (Receiver or Receiver and Manager appointed)",
    "Live Company", "Registration expired and has not been renewed", "Removed (Applied To Be Revoked)",
    "Removed (Dissolved)", "Revoked (Failed To Transfer Registration)", "Struck Off",
    "Struck Off (Early Dissolution - simplified winding up)", "Struck Off (Early Dissolution)",
    "Suspended", "Terminated", "To Be Ceased", "Under Judicial Management",
}


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str
    blocking: bool = True


def _one(con: duckdb.DuckDBPyConnection, sql: str, *params):
    row = con.execute(sql, list(params)).fetchall()
    return row[0][0] if row else None


def required_columns(con, table: str = "raw_acra") -> CheckResult:
    cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
    missing = [c for c in ACRA_REQUIRED_COLUMNS if c not in cols]
    return CheckResult("required_columns", not missing,
                       f"missing: {missing}" if missing else f"all {len(ACRA_REQUIRED_COLUMNS)} present")


def uen_unique_and_present(con, table: str = "raw_acra") -> CheckResult:
    nulls = _one(con, f"SELECT count(*) FROM {table} WHERE uen IS NULL OR trim(uen) IN ('', 'na')")
    dupes = _one(con, f"SELECT count(*) FROM (SELECT uen FROM {table} GROUP BY uen HAVING count(*) > 1)")
    ok = nulls == 0 and dupes == 0
    return CheckResult("uen_unique_and_present", ok, f"{nulls} blank UENs, {dupes} duplicated UENs")


def release_complete(con) -> CheckResult:
    """Every file must belong to the same publish date. If ACRA is half-way through
    republishing, some files are new and some are last month's, and the register would
    be a mix of two months. Wait for the rest instead."""
    rows = con.execute(
        "SELECT published_date, count(*) FROM release_files GROUP BY 1 ORDER BY 1").fetchall()
    detail = ", ".join(f"{d}: {n} files" for d, n in rows)
    return CheckResult("release_complete", len(rows) == 1, f"publish dates in release: {detail}")


def registration_dates_parse(con, table: str = "raw_acra", min_share: float = 0.999) -> CheckResult:
    total, ok = con.execute(
        f"SELECT count(*), count(try_cast(registration_incorporation_date AS DATE)) FROM {table}"
    ).fetchone()
    share = ok / total if total else 0.0
    return CheckResult("registration_dates_parse", share >= min_share,
                       f"{share:.4%} of {total:,} rows have a valid registration date (need {min_share:.1%})")


def row_count_drift(current: int, previous: int | None, max_drop: float = 0.02,
                    max_rise: float = 0.05) -> CheckResult:
    """The register only grows by a fraction of a percent a month. A big swing means a
    truncated download or a changed export, not two hundred thousand new companies."""
    if not previous:
        return CheckResult("row_count_drift", True, f"first release: {current:,} rows")
    change = current / previous - 1
    ok = -max_drop <= change <= max_rise
    return CheckResult("row_count_drift", ok,
                       f"{previous:,} -> {current:,} rows ({change:+.2%}; allowed {-max_drop:+.0%} to {max_rise:+.0%})")


def known_statuses(con, table: str = "raw_acra") -> CheckResult:
    seen = {r[0] for r in con.execute(f"SELECT DISTINCT entity_status_description FROM {table}").fetchall()}
    new = sorted(s for s in seen if s not in KNOWN_STATUSES)
    return CheckResult("known_statuses", not new, f"unrecognised statuses: {new}" if new else "all recognised",
                       blocking=False)


def run_release_checks(con, previous_rows: int | None) -> list[CheckResult]:
    current = int(_one(con, "SELECT count(*) FROM raw_acra") or 0)
    return [
        required_columns(con),
        release_complete(con),
        uen_unique_and_present(con),
        registration_dates_parse(con),
        row_count_drift(current, previous_rows),
        known_statuses(con),
    ]


class ContractViolation(RuntimeError):
    pass


def enforce(results: list[CheckResult]) -> None:
    failed = [r for r in results if r.blocking and not r.passed]
    if failed:
        raise ContractViolation("; ".join(f"{r.name}: {r.detail}" for r in failed))
