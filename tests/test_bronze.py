"""
Tests for bronze_pipeline.py  (run with: pytest -q tests/)

Tiny fake files in a temporary folder - no internet, no Spark and no real
Frost credentials needed. They check the promises from section 3 of the
project definition: files are stored unchanged, metadata is written,
duplicates are skipped, schema changes are flagged, failures are visible -
plus the Frost-specific logic (login, station groups, 404 = no data).
"""

import gzip
import io
import json
import os
import sys
import time
import urllib.error
import zipfile
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import bronze_pipeline as bp  # noqa: E402

MANUAL = {"source": "met", "dataset": "test_temp", "mode": "manual", "sep": ";", "required": True}
FROST_OBS = {"source": "met", "dataset": "frost_observations", "mode": "frost_observations", "sep": None,
             "required": True, "auth": "frost", "ext": "json",
             "url": "https://frost.example/obs?sources={sources}&referencetime={start}/{end}"}


@pytest.fixture
def landing(tmp_path, monkeypatch):
    """Point all folders to a temp dir and use one manual-only source."""
    monkeypatch.setattr(bp, "LANDING_DIR", str(tmp_path / "landing"))
    monkeypatch.setattr(bp, "MANUAL_DIR", str(tmp_path / "landing" / "_manual"))
    monkeypatch.setattr(bp, "MANIFEST_PATH", str(tmp_path / "landing" / "_manifest.csv"))
    monkeypatch.setattr(bp, "SOURCES", [MANUAL])
    folder = tmp_path / "landing" / "_manual" / "met" / "test_temp"
    folder.mkdir(parents=True)
    return folder


def http_error(code):
    return urllib.error.HTTPError("https://x", code, "test", {}, io.BytesIO(b""))


# --- storing files --------------------------------------------------------------

def test_file_is_stored_unchanged_with_metadata(landing):
    (landing / "a.csv").write_text("﻿Navn;Stasjon;Tid;Temp\r\nKjevik;SN39040;01.01.2024;-1,5\r\n",
                                   encoding="utf-8")
    result = bp.run_bronze()
    row = bp.read_manifest()[0]

    assert result["status"] == "success" and result["ingested"] == 1
    assert row["status"] == "ingested" and row["schema_status"] == "first_file"
    assert row["header"] == "Navn;Stasjon;Tid;Temp"                       # BOM/CRLF removed in metadata only
    stored = os.path.join(bp.LANDING_DIR, row["landing_path"])
    assert bp.sha256_of(stored) == bp.sha256_of(landing / "a.csv")        # byte-identical copy


def test_same_file_twice_is_skipped(landing):
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")
    bp.run_bronze()
    second = bp.run_bronze()
    assert second["ingested"] == 0 and second["skipped_duplicate"] == 1


def test_reverted_duplicate_uses_latest_manifest_entry(landing):
    source = landing / "timeseries.json"
    state = {"run_id": "run-a", "manifest": [],
             "counts": {"ingested": 0, "skipped_duplicate": 0}}

    for run_id, station_id in (("run-a", "SN1"), ("run-b", "SN2"), ("run-a-again", "SN1")):
        state["run_id"] = run_id
        source.write_text(json.dumps({"data": [{"sourceId": f"{station_id}:0"}]}), encoding="utf-8")
        bp.ingest_file(state, str(source), "met", "frost_timeseries", "snapshot", "test",
                       "frost_timeseries.json")

    first, _, reverted = state["manifest"]
    assert reverted["status"] == "skipped_duplicate"
    assert reverted["landing_path"] == first["landing_path"]
    assert bp.frost_station_ids(state) == ["SN1"]


def test_changed_header_is_flagged_but_kept(landing):
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")
    bp.run_bronze()
    (landing / "b.csv").write_text("A;B;C\n1;2;3\n", encoding="utf-8")
    bp.run_bronze()
    row = [r for r in bp.read_manifest() if r["original_filename"] == "b.csv"][0]
    assert row["status"] == "ingested" and row["schema_status"] == "changed"


def test_zip_is_kept_unpacked_and_checksummed_by_content(landing):
    def make_zip(path):                                    # same CSV inside, but a new ZIP timestamp each time
        with zipfile.ZipFile(path, "w") as zf:
            info = zipfile.ZipInfo("part-00000-abc.csv", date_time=time.localtime()[:6])
            zf.writestr(info, "START_TIME,QUANTITY_KWH\n2025-10-01T00:00:00.000+02:00,1.5\n")
    make_zip(landing / "export.zip")
    bp.run_bronze()
    row = bp.read_manifest()[0]
    stored = os.path.join(bp.LANDING_DIR, row["landing_path"])
    assert stored.endswith(".zip") and row["header"] == "START_TIME,QUANTITY_KWH"
    assert os.path.exists(stored[:-4] + "_unzipped/part-00000-abc.csv")   # unpacked copy for Spark

    time.sleep(2)
    (landing / "export.zip").unlink()
    make_zip(landing / "export2.zip")                                     # new ZIP bytes, same data
    assert bp.run_bronze()["skipped_duplicate"] == 1


def test_gzip_download_is_unpacked(tmp_path):
    path = tmp_path / "f.csv"
    path.write_bytes(gzip.compress(b"A,B\n1,2\n"))
    bp.gunzip_if_needed(str(path))
    assert path.read_bytes() == b"A,B\n1,2\n"
    bp.gunzip_if_needed(str(path))                                        # a plain file is left alone
    assert path.read_bytes() == b"A,B\n1,2\n"


# --- failures ---------------------------------------------------------------------

def test_failed_download_required_vs_optional(landing, monkeypatch):
    def broken_download(url, target, headers=None, attempts=3):
        raise OSError("no network")
    monkeypatch.setattr(bp, "download", broken_download)
    snap = {"source": "x", "dataset": "snap", "mode": "snapshot", "sep": ",", "url": "https://example.invalid"}

    monkeypatch.setattr(bp, "SOURCES", [dict(snap, required=True)])
    assert bp.run_bronze()["status"] == "failed"                          # required -> run fails

    monkeypatch.setattr(bp, "SOURCES", [dict(snap, required=False)])
    result = bp.run_bronze()
    assert result["status"] == "success" and result["warning"] == 1      # optional -> only a warning


def test_client_errors_are_not_retried(tmp_path, monkeypatch):
    calls = []
    def fake_urlopen(req, timeout=None):
        calls.append(req.full_url)
        raise http_error(404)
    monkeypatch.setattr(bp.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(urllib.error.HTTPError):
        bp.download("https://x", str(tmp_path / "f"))
    assert len(calls) == 1                                                # 404 -> no second attempt


# --- credentials (.env) and Frost ---------------------------------------------------

def test_env_file_is_loaded_but_does_not_override(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text('# comment\nFROST_CLIENT_ID="abc-123"\nexport OTHER=x=y\nALREADY=from_file\n', encoding="utf-8")
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)
    monkeypatch.delenv("OTHER", raising=False)
    monkeypatch.setenv("ALREADY", "from_github")
    bp.load_env_file([str(env)])
    assert os.environ["FROST_CLIENT_ID"] == "abc-123"                     # quotes removed
    assert os.environ["OTHER"] == "x=y"                                   # only the first "=" splits
    assert os.environ["ALREADY"] == "from_github"                         # real environment wins
    monkeypatch.delenv("OTHER")


def test_frost_login(monkeypatch):
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)
    assert bp.auth_headers({"auth": "frost"}) is None                     # missing
    monkeypatch.setenv("FROST_CLIENT_ID", "your-frost-client-id")
    assert bp.auth_headers({"auth": "frost"}) is None                     # placeholder from env.example
    monkeypatch.setenv("FROST_CLIENT_ID", "abc")
    import base64
    token = bp.auth_headers({"auth": "frost"})["Authorization"].split()[1]
    assert base64.b64decode(token) == b"abc:"                             # client id + empty password
    assert bp.auth_headers({}) == {}                                      # open sources need nothing


def test_missing_frost_login_fails_the_run_once(landing, monkeypatch):
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)
    monkeypatch.setattr(bp, "SOURCES", [FROST_OBS])
    result = bp.run_bronze()
    assert result["status"] == "failed" and result["failed"] == 1         # one clear failure, not hundreds
    assert "FROST_CLIENT_ID" in bp.read_manifest()[0]["status"]


def write_timeseries(state, ids):
    """Put a fake availableTimeSeries file in Bronze and the manifest."""
    path = os.path.join(bp.LANDING_DIR, "ts.json")
    os.makedirs(bp.LANDING_DIR, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"data": [{"sourceId": f"{i}:0"} for i in ids] + [{"sourceId": f"{ids[0]}:1"}]}, f)
    state["manifest"].append({"dataset": "frost_timeseries", "status": "ingested", "landing_path": "ts.json"})


def test_frost_requests_groups_months_and_skips(landing, monkeypatch):
    monkeypatch.setattr(bp, "ENV", "dev")
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-08")             # period from .env / environment
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")
    monkeypatch.setattr(bp, "FROST_STATIONS_PER_REQUEST", 2)
    state = {"manifest": []}
    write_timeseries(state, ["SN3", "SN1", "SN2"])                        # 3 stations -> groups of 2 -> 2 groups

    reqs = bp.frost_observation_requests(state, FROST_OBS)
    assert len(reqs) == 3 * 2                                             # 3 months x 2 groups
    url, month, filename = reqs[0]
    assert month == "2025-08" and filename.startswith("frost_obs_2025-08_l")
    assert filename.endswith("_g01.json")
    assert "sources=SN1,SN2" in url and "referencetime=2025-08-01/2025-09-01" in url  # sorted, ":0" removed

    state["manifest"] += [{"dataset": "frost_observations", "status": s, "original_filename": f}
                          for s, f in [("ingested", reqs[0][2]),
                                       ("no_data", reqs[1][2]),
                                       ("ingested", reqs[-2][2])]]
    names = [r[2] for r in bp.frost_observation_requests(state, FROST_OBS)]
    assert reqs[0][2] not in names                                        # stored -> skipped
    assert reqs[1][2] not in names                                        # known empty -> skipped
    assert reqs[-2][2] in names                                           # recent month -> asked again


def test_frost_station_membership_changes_request_identity(landing, monkeypatch):
    monkeypatch.setattr(bp, "ENV", "dev")
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-08")
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-08")
    monkeypatch.setattr(bp, "FROST_STATIONS_PER_REQUEST", 2)
    ids = ["SN1", "SN2", "SN3"]
    monkeypatch.setattr(bp, "frost_station_ids", lambda state: ids)
    state = {"manifest": []}

    original = bp.frost_observation_requests(state, FROST_OBS)
    state["manifest"] += [
        {"dataset": "frost_observations", "status": "ingested", "original_filename": filename}
        for _, _, filename in original
    ]
    ids.append("SN0")

    changed = bp.frost_observation_requests(state, FROST_OBS)
    assert len(changed) == 2
    assert {r[2] for r in changed}.isdisjoint({r[2] for r in original})


def test_frost_404_is_no_data_not_failure(landing, monkeypatch):
    monkeypatch.setenv("FROST_CLIENT_ID", "abc")
    monkeypatch.setattr(bp, "ENV", "dev")
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-10")
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")
    monkeypatch.setattr(bp, "SOURCES", [FROST_OBS])
    monkeypatch.setattr(bp, "frost_station_ids", lambda state: ["SN1"])
    def fake_download(url, target, headers=None, attempts=3):
        raise http_error(404)
    monkeypatch.setattr(bp, "download", fake_download)
    result = bp.run_bronze()
    assert result["status"] == "success" and result["no_data"] == 1


def test_month_range_from_environment(monkeypatch):
    monkeypatch.setattr(bp, "ENV", "prod")
    monkeypatch.setenv("NORGESPRIS_PROD_START_MONTH", "2023-11")
    monkeypatch.setenv("NORGESPRIS_PROD_END_MONTH", "")                     # empty = last complete month
    assert bp.month_range() == ("2023-11", None)

    monkeypatch.setattr(bp, "ENV", "dev")                                   # dev reads the DEV keys
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-09")
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")
    assert bp.month_range() == ("2025-09", "2025-10")


@pytest.mark.parametrize("start, end", [("", "2025-10"), ("2025-9", None), ("sept 2025", None), ("2025-09", "oct")])
def test_month_range_rejects_missing_or_wrong_format(monkeypatch, start, end):
    monkeypatch.setattr(bp, "ENV", "dev")
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", start)
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", end or "")
    with pytest.raises(ValueError, match="YYYY-MM"):
        bp.month_range()


def test_month_helpers():
    assert bp.month_list("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert bp.next_month("2025-12") == "2026-01"
    assert bp.last_complete_month(date(2026, 1, 15)) == "2025-12"
