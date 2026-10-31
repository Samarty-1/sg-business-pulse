"""Change detection in the extract step, against a fake data.gov.sg / SingStat."""
from __future__ import annotations

import json

import pytest

from pulse import extract
from pulse.extract import Http, Manifest


class FakeResponse:
    def __init__(self, payload=None, body: bytes = b"", status: int = 200):
        self.payload, self.body, self.status_code = payload, body, status

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise extract.requests.HTTPError(str(self.status_code), response=self)

    def iter_content(self, _size):
        yield self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeDataGov:
    """Serves one ACRA dataset whose publish time and bytes the test controls."""

    def __init__(self):
        self.headers = {}
        self.published = "2026-09-14T17:00:00+08:00"
        self.body = b"uen,entity_name\n1,A\n"
        self.downloads = 0
        self.fail_next = 0

    def get(self, url, **kw):
        if self.fail_next:
            self.fail_next -= 1
            return FakeResponse(status=429)
        if url.endswith("/collections/2/metadata"):
            return FakeResponse({"data": {"collectionMetadata": {"childDatasets": ["d_x"]}}})
        if url.endswith("/datasets/d_x/metadata"):
            return FakeResponse({"data": {"name": "ACRA ('X')", "lastUpdatedAt": self.published}})
        if url.endswith("/initiate-download"):
            return FakeResponse({"data": {}})
        if url.endswith("/poll-download"):
            return FakeResponse({"data": {"url": "https://s3.example/d_x.csv"}})
        if url == "https://s3.example/d_x.csv":
            self.downloads += 1
            return FakeResponse(body=self.body)
        raise AssertionError(f"unexpected url {url}")


@pytest.fixture
def fake(monkeypatch):
    monkeypatch.setattr(extract.time, "sleep", lambda _s: None)
    return FakeDataGov()


def land(tmp_path, fake):
    return extract.land_acra(tmp_path, Manifest(tmp_path / "manifest.json"), Http(session=fake, pause=0))


def test_first_run_downloads(tmp_path, fake):
    [x] = land(tmp_path, fake)
    assert x.changed and x.snapshot_date == "2026-09-14"
    assert (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").read_bytes() == fake.body


def test_unchanged_publish_time_skips_the_download(tmp_path, fake):
    land(tmp_path, fake)
    [x] = land(tmp_path, fake)
    assert not x.changed and fake.downloads == 1


def test_republished_identical_bytes_lands_nothing(tmp_path, fake):
    land(tmp_path, fake)
    fake.published = "2026-10-14T17:00:00+08:00"
    [x] = land(tmp_path, fake)
    assert not x.changed and x.snapshot_date == "2026-09-14"
    assert not (tmp_path / "landing/acra/snapshot=2026-10-14/d_x.csv").exists()


def test_new_content_lands_as_a_new_snapshot(tmp_path, fake):
    land(tmp_path, fake)
    fake.published, fake.body = "2026-10-14T17:00:00+08:00", b"uen,entity_name\n1,A\n2,B\n"
    [x] = land(tmp_path, fake)
    assert x.changed and x.snapshot_date == "2026-10-14"
    # the earlier snapshot is immutable and still there
    assert (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").exists()
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["d_x"]["path"].endswith("snapshot=2026-10-14/d_x.csv")


def test_rate_limits_are_retried(tmp_path, fake):
    fake.fail_next = 2
    [x] = land(tmp_path, fake)
    assert x.changed


def test_singstat_pages_are_stitched_together(monkeypatch):
    monkeypatch.setattr(extract.time, "sleep", lambda _s: None)
    points = [{"key": f"2025 {m}", "value": str(i)} for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"])]

    class Pages:
        headers = {}

        def get(self, url, params, **kw):
            chunk = points[params["offset"]:params["offset"] + params["limit"]]
            return FakeResponse({"Data": {"id": "T", "title": "t", "dataLastUpdated": "05/10/2026",
                                          "frequency": "Monthly",
                                          "row": [{"seriesNo": "1", "rowText": "Total", "columns": chunk}]}})

    table = extract.fetch_singstat_table(Http(session=Pages(), pause=0), "T", page=3)
    assert [c["key"] for c in table["row"][0]["columns"]] == [p["key"] for p in points]


def test_missing_local_file_is_downloaded_again(tmp_path, fake):
    """A fresh CI runner restores the manifest but not the landed files."""
    land(tmp_path, fake)
    (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").unlink()
    [x] = land(tmp_path, fake)
    assert fake.downloads == 2
    assert not x.changed  # same content as before: restored, not a new release
    assert (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").read_bytes() == fake.body


def test_restore_after_republish_lands_under_the_original_snapshot(tmp_path, fake):
    land(tmp_path, fake)
    (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").unlink()
    fake.published = "2026-10-14T17:00:00+08:00"  # republished, same bytes
    [x] = land(tmp_path, fake)
    assert not x.changed and x.snapshot_date == "2026-09-14"
    assert (tmp_path / "landing/acra/snapshot=2026-09-14/d_x.csv").exists()
    assert not (tmp_path / "landing/acra/snapshot=2026-10-14/d_x.csv").exists()
