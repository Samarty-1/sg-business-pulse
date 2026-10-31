"""Tiny hand-written ACRA releases, laid out on disk exactly like the real landing zone."""
from __future__ import annotations

import csv
import json
from pathlib import Path

COLUMNS = [
    "uen", "issuance_agency_id", "entity_name", "entity_type_description",
    "business_constitution_description", "company_type_description",
    "entity_status_description", "registration_incorporation_date", "postal_code",
    "primary_ssic_code", "primary_ssic_description",
]


def entity(uen, name, status="Live Company", reg="2020-03-01", postal="238801", ssic="56111",
           kind="Local Company"):
    return {
        "uen": uen, "issuance_agency_id": "ACRA", "entity_name": name,
        "entity_type_description": kind,
        "business_constitution_description": "Sole-Proprietor" if kind.startswith("Sole") else "na",
        "company_type_description": "na" if kind.startswith("Sole") else "Exempt Private Company Limited by Shares",
        "entity_status_description": status, "registration_incorporation_date": reg,
        "postal_code": postal, "primary_ssic_code": ssic, "primary_ssic_description": "TEST",
    }


# Release 1: four entities.
R1 = [
    entity("201900001A", "KOPI CORNER PTE. LTD."),
    entity("201900002B", "LAKSA HOUSE PTE. LTD.", ssic="56112"),
    entity("53000003C", "TAN HARDWARE", kind="Sole Proprietorship/ Partnership", status="Live",
           ssic="47521", postal="560123", reg="2016-07-01"),
    entity("201900004D", "BYTE WORKS PTE. LTD.", ssic="62011", postal="018989"),
]

# Release 2: one struck off, one renamed, one removed, one new, one untouched.
R2 = [
    entity("201900001A", "KOPI CORNER PTE. LTD.", status="Struck Off"),
    entity("201900002B", "LAKSA HOUSE SINGAPORE PTE. LTD.", ssic="56112"),
    entity("53000003C", "TAN HARDWARE", kind="Sole Proprietorship/ Partnership", status="Live",
           ssic="47521", postal="560123", reg="2016-07-01"),
    entity("202600005E", "BUBBLE TEA BAR PTE. LTD.", ssic="56123", reg="2026-01-15"),
]

# Release 3: the removed entity comes back.
R3 = R2 + [entity("201900004D", "BYTE WORKS PTE. LTD.", ssic="62011", postal="018989")]


def write_release(root: Path, snapshot: str, rows: list[dict], files: int = 2) -> None:
    """Split a release across `files` CSVs (ACRA ships 27) and record them in the manifest."""
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for i in range(files):
        dataset_id = f"d_test{i}"
        dest = root / "landing" / "acra" / f"snapshot={snapshot}" / f"{dataset_id}.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(rows[i::files])
        manifest[dataset_id] = {"source": "acra", "name": f"test {i}", "published": snapshot,
                                "snapshot_date": snapshot, "sha256": "x",
                                "path": dest.relative_to(root).as_posix()}
    manifest_path.write_text(json.dumps(manifest))


def write_singstat(root: Path, snapshot: str = "2026-10-05") -> None:
    table = {"id": "M602131", "title": "F&B Services Index", "dataLastUpdated": "05/10/2026",
             "frequency": "Monthly",
             "row": [{"seriesNo": "1", "rowText": "Total",
                      "columns": [{"key": f"{y} {m}", "value": str(90 + i)}
                                  for i, (y, m) in enumerate((y, m) for y in (2024, 2025)
                                                             for m in ("Jan", "Feb", "Mar"))]}]}
    dest = root / "landing" / "singstat" / f"snapshot={snapshot}" / "M602131.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(table))
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    manifest["M602131"] = {"source": "singstat", "name": "fnb_services_index", "published": "05/10/2026",
                           "snapshot_date": snapshot, "sha256": "x", "path": dest.relative_to(root).as_posix()}
    manifest_path.write_text(json.dumps(manifest))
