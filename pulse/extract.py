"""Land source files, downloading only what changed.

Each dataset's publish time is checked against a manifest of what was landed before.
Unchanged files are skipped without downloading. A file that was republished with
identical bytes is recognised by its SHA-256 and also skipped.

Landed files are immutable: `data/landing/<source>/snapshot=<date>/<dataset>.<ext>`,
where the snapshot date is the date the *source* published the file, not the date
the pipeline ran. Re-running on the same day, or a week later, lands nothing new.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from .sources import (
    ACRA_COLLECTION_ID,
    DATA_GOV_API,
    DATA_GOV_DOWNLOAD_API,
    SINGSTAT_API,
    SINGSTAT_TABLES,
)

log = logging.getLogger(__name__)

USER_AGENT = "sg-business-pulse (+https://github.com/Samarty-1/sg-business-pulse)"


@dataclass(frozen=True)
class Landed:
    source: str
    dataset_id: str
    name: str
    snapshot_date: str
    path: Path | None  # None when the dataset was unchanged and nothing was written
    changed: bool


class Http:
    """requests with retries, backoff and polite rate limiting."""

    def __init__(self, session: requests.Session | None = None, retries: int = 6, pause: float = 0.3):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.retries = retries
        self.pause = pause

    def get(self, url: str, **kw) -> requests.Response:
        kw.setdefault("timeout", 60)
        for attempt in range(self.retries):
            try:
                r = self.session.get(url, **kw)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"{r.status_code} from {url}", response=r)
                r.raise_for_status()
                time.sleep(self.pause)
                return r
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500 and status != 429:
                    raise
                if attempt == self.retries - 1:
                    raise
                wait = min(60, 2 ** attempt * 2)
                log.warning("retrying %s in %ss (%s)", url, wait, e)
                time.sleep(wait)
        raise AssertionError("unreachable")


class Manifest:
    """What has been landed so far: one entry per dataset."""

    def __init__(self, path: Path):
        self.path = path
        self.entries: dict[str, dict] = json.loads(path.read_text()) if path.exists() else {}

    def get(self, dataset_id: str) -> dict | None:
        return self.entries.get(dataset_id)

    def put(self, dataset_id: str, **entry) -> None:
        self.entries[dataset_id] = entry

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.entries, indent=2, sort_keys=True))
        tmp.replace(self.path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------- data.gov.sg (ACRA)

def acra_datasets(http: Http) -> list[str]:
    r = http.get(f"{DATA_GOV_API}/collections/{ACRA_COLLECTION_ID}/metadata")
    ids = r.json()["data"]["collectionMetadata"]["childDatasets"]
    if not ids:
        raise RuntimeError("ACRA collection lists no datasets")
    return list(ids)


def dataset_metadata(http: Http, dataset_id: str) -> dict:
    return http.get(f"{DATA_GOV_API}/datasets/{dataset_id}/metadata").json()["data"]


def download_url(http: Http, dataset_id: str, max_polls: int = 30) -> str:
    """data.gov.sg prepares large files asynchronously: initiate, then poll."""
    base = f"{DATA_GOV_DOWNLOAD_API}/datasets/{dataset_id}"
    data = http.get(f"{base}/initiate-download").json().get("data", {})
    for _ in range(max_polls):
        if data.get("url"):
            return data["url"]
        time.sleep(3)
        data = http.get(f"{base}/poll-download").json().get("data", {})
    raise TimeoutError(f"download of {dataset_id} was never ready")


def _stream(http: Http, url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with http.get(url, stream=True, timeout=300) as r, tmp.open("wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
    tmp.replace(dest)


def land_acra(root: Path, manifest: Manifest, http: Http | None = None) -> list[Landed]:
    http = http or Http()
    out: list[Landed] = []
    for dataset_id in acra_datasets(http):
        meta = dataset_metadata(http, dataset_id)
        published = meta["lastUpdatedAt"]
        snapshot = published[:10]
        prev = manifest.get(dataset_id)
        if prev and prev["published"] == published and (root / prev["path"]).exists():
            log.info("unchanged: %s", meta["name"])
            out.append(Landed("acra", dataset_id, meta["name"], prev["snapshot_date"], None, False))
            continue

        dest = root / "landing" / "acra" / f"snapshot={snapshot}" / f"{dataset_id}.csv"
        _stream(http, download_url(http, dataset_id), dest)
        digest = sha256(dest)
        if prev and prev["sha256"] == digest:
            # Same bytes as last time. Either the source republished without changing
            # anything, or this run restored a file missing locally. In both cases the
            # earlier snapshot stays the latest truth.
            prev_path = root / prev["path"]
            if dest != prev_path:
                if prev_path.exists():
                    dest.unlink()
                else:
                    prev_path.parent.mkdir(parents=True, exist_ok=True)
                    dest.replace(prev_path)
            log.info("no new content: %s", meta["name"])
            manifest.put(dataset_id, **{**prev, "published": published})
            out.append(Landed("acra", dataset_id, meta["name"], prev["snapshot_date"], None, False))
            continue

        manifest.put(dataset_id, source="acra", name=meta["name"], published=published, snapshot_date=snapshot,
                     sha256=digest, path=dest.relative_to(root).as_posix())
        log.info("landed: %s -> %s", meta["name"], dest)
        out.append(Landed("acra", dataset_id, meta["name"], snapshot, dest, True))
    manifest.save()
    return out


# ---------------------------------------------------------------- SingStat

def fetch_singstat_table(http: Http, table_id: str, page: int = 3000) -> dict:
    """The API caps each response at `limit` data points, so page through with offset."""
    rows: dict[str, dict] = {}
    offset, meta = 0, None
    while True:
        d = http.get(f"{SINGSTAT_API}/tabledata/{table_id}", params={"limit": page, "offset": offset}).json()["Data"]
        meta = meta or {k: d[k] for k in ("id", "title", "dataLastUpdated", "frequency")}
        got = 0
        for r in d["row"]:
            entry = rows.setdefault(r["seriesNo"], {"seriesNo": r["seriesNo"], "rowText": r["rowText"], "columns": []})
            entry["columns"].extend(r["columns"])
            got += len(r["columns"])
        if got < page:
            break
        offset += page
    return {**meta, "row": list(rows.values())}


def land_singstat(root: Path, manifest: Manifest, http: Http | None = None) -> list[Landed]:
    http = http or Http()
    out: list[Landed] = []
    for table_id, name in SINGSTAT_TABLES.items():
        table = fetch_singstat_table(http, table_id)
        day, month, year = table["dataLastUpdated"].split("/")
        snapshot = f"{year}-{month}-{day}"
        body = json.dumps(table, sort_keys=True).encode()
        digest = hashlib.sha256(body).hexdigest()
        prev = manifest.get(table_id)
        if prev and prev["sha256"] == digest and (root / prev["path"]).exists():
            out.append(Landed("singstat", table_id, name, prev["snapshot_date"], None, False))
            continue
        dest = root / "landing" / "singstat" / f"snapshot={snapshot}" / f"{table_id}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        manifest.put(table_id, source="singstat", name=name, published=table["dataLastUpdated"], snapshot_date=snapshot,
                     sha256=digest, path=dest.relative_to(root).as_posix())
        out.append(Landed("singstat", table_id, name, snapshot, dest, True))
    manifest.save()
    return out
