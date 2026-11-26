"""
Tests for bronze_pipeline.py  (run with: pytest -q tests/)

Tiny fake files in a temporary folder - no internet, no Spark and no real
Frost credentials needed. They check the promises from section 3 of the
project definition: files are stored unchanged, metadata is written,
duplicates are skipped, schema changes are flagged, failures are visible -
plus the Frost-specific logic (login, station groups, 404 = no data) and the
delta load (only the newest month, fallback to the full file).
"""

import gzip                                                     # compress test data like data.elhub.no
import io                                                       # fake HTTP error bodies
import json                                                     # write fake Frost JSON
import os                                                       # paths and environment variables
import sys                                                      # change the import path
import time                                                     # different ZIP timestamps
import urllib.error                                             # the HTTPError class
import zipfile                                                  # build test ZIP files
from datetime import date                                       # fixed dates for month tests

import pytest                                                   # test framework

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import bronze_pipeline as bp  # noqa: E402

MANUAL = {"source": "met", "dataset": "test_temp", "mode": "manual", "sep": ";", "required": True}  # a source that only reads _manual files
FROST_OBS = {"source": "met", "dataset": "frost_observations", "mode": "frost_observations", "sep": None,  # a Frost observations source ...
             "required": True, "auth": "frost", "ext": "json",  # ... that needs a login ...
             "url": "https://frost.example/obs?sources={sources}&referencetime={start}/{end}"}  # ... with a fake URL


@pytest.fixture                                                 # new temp folders for every test
def landing(tmp_path, monkeypatch):                             # test landing folder
    """Point all folders to a temp dir and use one manual-only source."""
    monkeypatch.setattr(bp, "LANDING_DIR", str(tmp_path / "landing"))  # landing in the temp folder
    monkeypatch.setattr(bp, "MANUAL_DIR", str(tmp_path / "landing" / "_manual"))  # manual folder in the temp folder
    monkeypatch.setattr(bp, "MANIFEST_PATH", str(tmp_path / "landing" / "_manifest.csv"))  # manifest in the temp folder
    monkeypatch.setattr(bp, "SOURCES", [MANUAL])                # only the manual test source
    folder = tmp_path / "landing" / "_manual" / "met" / "test_temp"  # folder for manual test files
    folder.mkdir(parents=True)                                  # create it
    return folder                                               # the test puts its files here


def http_error(code):                                           # build a fake HTTP error
    return urllib.error.HTTPError("https://x", code, "test", {}, io.BytesIO(b""))  # HTTPError with the given code


# --- storing files --------------------------------------------------------------

def test_file_is_stored_unchanged_with_metadata(landing):       # stored unchanged + metadata
    (landing / "a.csv").write_text("﻿Navn;Stasjon;Tid;Temp\r\nKjevik;SN39040;01.01.2024;-1,5\r\n",  # CSV with BOM and Windows line ends ...
                                   encoding="utf-8")            # ... written as UTF-8
    result = bp.run_bronze()                                    # run the landing stage
    row = bp.read_manifest()[0]                                 # the manifest row of the file

    assert result["status"] == "success" and result["ingested"] == 1  # one file stored
    assert row["status"] == "ingested" and row["schema_status"] == "first_file"  # first file of this dataset
    assert row["header"] == "Navn;Stasjon;Tid;Temp"                       # BOM/CRLF removed in metadata only
    stored = os.path.join(bp.LANDING_DIR, row["landing_path"])  # path of the stored copy
    assert bp.sha256_of(stored) == bp.sha256_of(landing / "a.csv")        # byte-identical copy


def test_same_file_twice_is_skipped(landing):                   # the same file twice
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")  # a small CSV
    bp.run_bronze()                                             # first run stores it
    second = bp.run_bronze()                                    # second run sees it again
    assert second["ingested"] == 0 and second["skipped_duplicate"] == 1  # stored once, skipped once


def test_reverted_duplicate_uses_latest_manifest_entry(landing):  # A, B, then A again
    source = landing / "timeseries.json"                        # the file that changes
    state = {"run_id": "run-a", "manifest": [],                 # minimal state for ingest_file ...
             "counts": {"ingested": 0, "skipped_duplicate": 0}}  # ... with counters

    for run_id, station_id in (("run-a", "SN1"), ("run-b", "SN2"), ("run-a-again", "SN1")):  # three versions of the file
        state["run_id"] = run_id                                # a new run id each time
        source.write_text(json.dumps({"data": [{"sourceId": f"{station_id}:0"}]}), encoding="utf-8")  # write this version
        bp.ingest_file(state, str(source), "met", "frost_timeseries", "snapshot", "test",  # store it ...
                       "frost_timeseries.json")                 # ... under this name

    first, _, reverted = state["manifest"]                      # the three manifest rows
    assert reverted["status"] == "skipped_duplicate"            # the third is a duplicate ...
    assert reverted["landing_path"] == first["landing_path"]    # ... of the first one
    assert bp.frost_station_ids(state) == ["SN1"]               # the newest content is used


def test_changed_header_is_flagged_but_kept(landing):           # a new column is noticed
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")  # first file: two columns
    bp.run_bronze()                                             # store it
    (landing / "b.csv").write_text("A;B;C\n1;2;3\n", encoding="utf-8")  # second file: three columns
    bp.run_bronze()                                             # store it
    row = [r for r in bp.read_manifest() if r["original_filename"] == "b.csv"][0]  # manifest row of the second file
    assert row["status"] == "ingested" and row["schema_status"] == "changed"  # stored, but flagged


def test_zip_is_kept_unpacked_and_checksummed_by_content(landing):  # ZIP handling
    def make_zip(path):                                    # same CSV inside, but a new ZIP timestamp each time
        with zipfile.ZipFile(path, "w") as zf:                  # write a ZIP ...
            info = zipfile.ZipInfo("part-00000-abc.csv", date_time=time.localtime()[:6])  # ... with one CSV inside ...
            zf.writestr(info, "START_TIME,QUANTITY_KWH\n2025-10-01T00:00:00.000+02:00,1.5\n")  # ... with this content
    make_zip(landing / "export.zip")                            # first ZIP
    bp.run_bronze()                                             # store it
    row = bp.read_manifest()[0]                                 # its manifest row
    stored = os.path.join(bp.LANDING_DIR, row["landing_path"])  # path of the stored ZIP
    assert stored.endswith(".zip") and row["header"] == "START_TIME,QUANTITY_KWH"  # kept as ZIP, header from the CSV inside
    assert os.path.exists(stored[:-4] + "_unzipped/part-00000-abc.csv")   # unpacked copy for Spark

    time.sleep(2)                                               # wait, so the new ZIP gets another timestamp
    (landing / "export.zip").unlink()                           # remove the first ZIP
    make_zip(landing / "export2.zip")                                     # new ZIP bytes, same data
    assert bp.run_bronze()["skipped_duplicate"] == 1            # same content -> skipped


def test_gzip_download_is_unpacked(tmp_path):                   # gzip handling
    path = tmp_path / "f.csv"                                   # a test file
    path.write_bytes(gzip.compress(b"A,B\n1,2\n"))              # gzip-compressed content
    bp.gunzip_if_needed(str(path))                              # unpack it
    assert path.read_bytes() == b"A,B\n1,2\n"                   # plain content now
    bp.gunzip_if_needed(str(path))                                        # a plain file is left alone
    assert path.read_bytes() == b"A,B\n1,2\n"                   # unchanged


# --- failures ---------------------------------------------------------------------

def test_failed_download_required_vs_optional(landing, monkeypatch):  # required vs optional sources
    def broken_download(url, target, headers=None, attempts=3):  # a download that always fails
        raise OSError("no network")                             # network error
    monkeypatch.setattr(bp, "download", broken_download)        # use it instead of the real download
    snap = {"source": "x", "dataset": "snap", "mode": "snapshot", "sep": ",", "url": "https://example.invalid"}  # a snapshot test source

    monkeypatch.setattr(bp, "SOURCES", [dict(snap, required=True)])  # as required source
    assert bp.run_bronze()["status"] == "failed"                          # required -> run fails

    monkeypatch.setattr(bp, "SOURCES", [dict(snap, required=False)])  # as optional source
    result = bp.run_bronze()                                    # run the landing stage
    assert result["status"] == "success" and result["warning"] == 1      # optional -> only a warning


def test_client_errors_are_not_retried(tmp_path, monkeypatch):  # 4xx answers are final
    calls = []                                                  # URLs that were requested
    def fake_urlopen(req, timeout=None):                        # fake urlopen
        calls.append(req.full_url)                              # remember the call
        raise http_error(404)                                   # answer 404
    monkeypatch.setattr(bp.urllib.request, "urlopen", fake_urlopen)  # use it instead of the real urlopen
    with pytest.raises(urllib.error.HTTPError):                 # expect the error ...
        bp.download("https://x", str(tmp_path / "f"))           # ... from download()
    assert len(calls) == 1                                                # 404 -> no second attempt


# --- credentials (.env) and Frost ---------------------------------------------------

def test_env_file_is_loaded_but_does_not_override(tmp_path, monkeypatch):  # .env loading
    env = tmp_path / ".env"                                     # a temp .env file
    env.write_text('# comment\nFROST_CLIENT_ID="abc-123"\nexport OTHER=x=y\nALREADY=from_file\n', encoding="utf-8")  # comment, quotes, export, value with =
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)        # not set before
    monkeypatch.delenv("OTHER", raising=False)                  # not set before
    monkeypatch.setenv("ALREADY", "from_github")                # already set (like a GitHub secret)
    bp.load_env_file([str(env)])                                # read the test .env
    assert os.environ["FROST_CLIENT_ID"] == "abc-123"                     # quotes removed
    assert os.environ["OTHER"] == "x=y"                                   # only the first "=" splits
    assert os.environ["ALREADY"] == "from_github"                         # real environment wins
    monkeypatch.delenv("OTHER")                                 # clean up


def test_frost_login(monkeypatch):                              # Frost login header
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)        # no client id
    assert bp.auth_headers({"auth": "frost"}) is None                     # missing
    monkeypatch.setenv("FROST_CLIENT_ID", "your-frost-client-id")  # the placeholder
    assert bp.auth_headers({"auth": "frost"}) is None                     # placeholder from env.example
    monkeypatch.setenv("FROST_CLIENT_ID", "abc")                # a real-looking id
    import base64                                               # to decode the header
    token = bp.auth_headers({"auth": "frost"})["Authorization"].split()[1]  # the Basic token
    assert base64.b64decode(token) == b"abc:"                             # client id + empty password
    assert bp.auth_headers({}) == {}                                      # open sources need nothing


def test_missing_frost_login_fails_the_run_once(landing, monkeypatch):  # missing login -> one failure
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)        # no client id
    monkeypatch.setattr(bp, "SOURCES", [FROST_OBS])             # only the Frost observations source
    result = bp.run_bronze()                                    # run the landing stage
    assert result["status"] == "failed" and result["failed"] == 1         # one clear failure, not hundreds
    assert "FROST_CLIENT_ID" in bp.read_manifest()[0]["status"]  # the manifest says why


def write_timeseries(state, ids):                               # helper: fake station list in landing
    """Put a fake availableTimeSeries file in Bronze and the manifest."""
    path = os.path.join(bp.LANDING_DIR, "ts.json")              # file in the test landing folder
    os.makedirs(bp.LANDING_DIR, exist_ok=True)                  # create the folder
    with open(path, "w", encoding="utf-8") as f:                # write the JSON ...
        json.dump({"data": [{"sourceId": f"{i}:0"} for i in ids] + [{"sourceId": f"{ids[0]}:1"}]}, f)  # ... stations + one station twice
    state["manifest"].append({"dataset": "frost_timeseries", "status": "ingested", "landing_path": "ts.json"})  # add it to the manifest


def test_frost_requests_groups_months_and_skips(landing, monkeypatch):  # request list: months x groups, skips
    monkeypatch.setattr(bp, "LOAD_MODE", "full")                # full mode: the whole period (delta is tested below)
    monkeypatch.setattr(bp, "ENV", "dev")                       # dev environment
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-08")             # period from .env / environment
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")   # to October
    monkeypatch.setattr(bp, "FROST_STATIONS_PER_REQUEST", 2)    # small groups for the test
    state = {"manifest": []}                                    # empty manifest
    write_timeseries(state, ["SN3", "SN1", "SN2"])                        # 3 stations -> groups of 2 -> 2 groups

    reqs = bp.frost_observation_requests(state, FROST_OBS)      # build the requests
    assert len(reqs) == 3 * 2                                             # 3 months x 2 groups
    url, month, filename = reqs[0]                              # the first request
    assert month == "2025-08" and filename.startswith("frost_obs_2025-08_l")  # first month, file name pattern
    assert filename.endswith("_g01.json")                       # first group
    assert "sources=SN1,SN2" in url and "referencetime=2025-08-01/2025-09-01" in url  # sorted, ":0" removed

    state["manifest"] += [{"dataset": "frost_observations", "status": s, "original_filename": f}  # pretend some files exist:
                          for s, f in [("ingested", reqs[0][2]),  # first: stored
                                       ("no_data", reqs[1][2]),  # second: known empty
                                       ("ingested", reqs[-2][2])]]  # second last: stored, but recent
    names = [r[2] for r in bp.frost_observation_requests(state, FROST_OBS)]  # build the requests again
    assert reqs[0][2] not in names                                        # stored -> skipped
    assert reqs[1][2] not in names                                        # known empty -> skipped
    assert reqs[-2][2] in names                                           # recent month -> asked again


def test_frost_station_membership_changes_request_identity(landing, monkeypatch):  # new station -> new file names
    monkeypatch.setattr(bp, "LOAD_MODE", "full")                # full mode: the whole period
    monkeypatch.setattr(bp, "ENV", "dev")                       # dev environment
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-08")  # one month
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-08")   # one month
    monkeypatch.setattr(bp, "FROST_STATIONS_PER_REQUEST", 2)    # small groups for the test
    ids = ["SN1", "SN2", "SN3"]                                 # station list
    monkeypatch.setattr(bp, "frost_station_ids", lambda state: ids)  # use it instead of the real one
    state = {"manifest": []}                                    # empty manifest

    original = bp.frost_observation_requests(state, FROST_OBS)  # requests with 3 stations
    state["manifest"] += [                                      # pretend they were all stored ...
        {"dataset": "frost_observations", "status": "ingested", "original_filename": filename}  # ... one row each ...
        for _, _, filename in original                          # ... for every request
    ]                                                           # end of the rows
    ids.append("SN0")                                           # a fourth station appears

    changed = bp.frost_observation_requests(state, FROST_OBS)   # requests again
    assert len(changed) == 2                                    # two groups again
    assert {r[2] for r in changed}.isdisjoint({r[2] for r in original})  # new names: nothing is skipped


def test_frost_404_is_no_data_not_failure(landing, monkeypatch):  # 404 from Frost = no data
    monkeypatch.setenv("FROST_CLIENT_ID", "abc")                # a client id
    monkeypatch.setattr(bp, "ENV", "dev")                       # dev environment
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-10")  # one month
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")   # one month
    monkeypatch.setattr(bp, "SOURCES", [FROST_OBS])             # only the Frost observations source
    monkeypatch.setattr(bp, "frost_station_ids", lambda state: ["SN1"])  # one station
    def fake_download(url, target, headers=None, attempts=3):   # fake download ...
        raise http_error(404)                                   # ... that answers 404
    monkeypatch.setattr(bp, "download", fake_download)          # use it
    result = bp.run_bronze()                                    # run the landing stage
    assert result["status"] == "success" and result["no_data"] == 1  # success, counted as no data


def test_month_range_from_environment(monkeypatch):             # period from the environment
    monkeypatch.setattr(bp, "ENV", "prod")                      # prod environment
    monkeypatch.setenv("NORGESPRIS_PROD_START_MONTH", "2023-11")  # prod start
    monkeypatch.setenv("NORGESPRIS_PROD_END_MONTH", "")                     # empty = last complete month
    assert bp.month_range() == ("2023-11", None)                # start, no end

    monkeypatch.setattr(bp, "ENV", "dev")                                   # dev reads the DEV keys
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", "2025-09")  # dev start
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", "2025-10")   # dev end
    assert bp.month_range() == ("2025-09", "2025-10")           # both months


@pytest.mark.parametrize("start, end", [("", "2025-10"), ("2025-9", None), ("sept 2025", None), ("2025-09", "oct")])  # four wrong settings
def test_month_range_rejects_missing_or_wrong_format(monkeypatch, start, end):  # each must raise an error
    monkeypatch.setattr(bp, "ENV", "dev")                       # dev environment
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", start)     # test start value
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", end or "")   # test end value
    with pytest.raises(ValueError, match="YYYY-MM"):            # expect a clear error ...
        bp.month_range()                                        # ... from month_range()


def test_month_helpers():                                       # month helper functions
    assert bp.month_list("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]  # months over a year change
    assert bp.next_month("2025-12") == "2026-01"                # December -> January
    assert bp.last_complete_month(date(2026, 1, 15)) == "2025-12"  # January -> December last year


# --- delta load (NORGESPRIS_LOAD_MODE=delta) -----------------------------------------------

ELHUB_DELTA = {"source": "elhub", "dataset": "test_cons", "mode": "snapshot", "sep": ";", "required": True,  # a source ...
               "url": "https://api.example/full.csv",           # ... with a full file ...
               "delta_url": "https://files.example/test_cons-all-en-{month}-00.csv"}  # ... and one file per month


def delta_period(monkeypatch, start="2026-09", end="2026-10", months=1):  # helper: fixed period, delta mode
    monkeypatch.setattr(bp, "ENV", "dev")                       # dev environment
    monkeypatch.setenv("NORGESPRIS_DEV_START_MONTH", start)     # first month of the period
    monkeypatch.setenv("NORGESPRIS_DEV_END_MONTH", end)         # last month (fixed, so the test does not depend on today)
    monkeypatch.setattr(bp, "LOAD_MODE", "delta")               # delta mode
    monkeypatch.setattr(bp, "DELTA_MONTHS", months)             # how many newest months at least


def row(period, ingested="2026-11-08T10:00:00+00:00", status="ingested", dataset="test_cons"):  # helper: one manifest row
    return {"dataset": dataset, "period": period, "status": status, "ingested_at_utc": ingested}  # the fields delta_months uses


def test_delta_url_fills_the_month():                           # month in the data.elhub.no file name
    cons = next(c for c in bp.SOURCES if c["dataset"] == "consumption_per_group_municipality_hour")  # the real registry entry
    assert bp.delta_url(cons, "2026-10").endswith("consumption_per_group_municipality_hour-all-en-2026-10-00.csv")  # day 00 = whole month


def test_delta_months_high_water_mark(monkeypatch):             # which months a delta run loads
    delta_period(monkeypatch, start="2026-01", end="2026-10")   # period Jan-Oct 2026
    state = {"manifest": []}                                    # nothing in landing yet
    assert bp.delta_months(state, "test_cons") is None          # -> full load first

    state["manifest"] = [row("snapshot", "2026-11-08T10:00:00+00:00")]  # full file from 8 Nov (complete up to Oct)
    assert bp.delta_months(state, "test_cons") == ["2026-10"]   # only last month

    state["manifest"] = [row("snapshot", "2026-08-20T10:00:00+00:00")]  # full file from August (complete up to July)
    assert bp.delta_months(state, "test_cons") == ["2026-07", "2026-08", "2026-09", "2026-10"]  # the gap is filled

    state["manifest"] = [row("2026-10"), row("2026-03", status="failed: HTTP 500"), row("2026-01", dataset="other")]  # month file + noise
    assert bp.delta_months(state, "test_cons") == ["2026-10"]   # failed rows and other datasets do not count
    monkeypatch.setattr(bp, "DELTA_MONTHS", 2)                  # always the 2 newest months
    assert bp.delta_months(state, "test_cons") == ["2026-09", "2026-10"]  # two months


def fake_source(calls, month_header="A;B"):                     # helper: fake download for the full and the month file
    def download(url, target, headers=None, attempts=3):        # same signature as bp.download
        calls.append(url)                                       # remember what was asked for
        if url.endswith("-00.csv"):                             # a month file
            text = f"{month_header}\r\n10;20\r\n"            # one row (header can be changed by the test)
        else:                                                   # the full file
            text = "A;B\r\n1;2\r\n3;4\r\n"                  # two rows
        with open(target, "w", encoding="utf-8") as f:          # write the fake answer
            f.write(text)                                       # as the downloaded file
    return download                                             # the fake function


def test_delta_run_loads_full_first_then_only_last_month(landing, monkeypatch):  # first run full, then delta
    delta_period(monkeypatch)                                   # period Sep-Oct 2026, delta mode
    monkeypatch.setattr(bp, "SOURCES", [ELHUB_DELTA])           # only the test source
    calls = []                                                  # URLs asked for
    monkeypatch.setattr(bp, "download", fake_source(calls))     # fake internet

    assert bp.run_bronze()["status"] == "success"               # first run
    assert calls == ["https://api.example/full.csv"]            # nothing in landing -> the full file

    calls.clear()                                               # second run
    assert bp.run_bronze()["status"] == "success"               # runs fine
    assert calls == ["https://files.example/test_cons-all-en-2026-10-00.csv"]  # only the newest month of the period
    month = [r for r in bp.read_manifest() if r["period"] == "2026-10"]  # its manifest row
    assert month[0]["status"] == "ingested" and month[0]["original_filename"] == "test_cons_2026-10.csv"  # stored as its own file
    assert month[0]["schema_status"] == "ok"                    # same columns as the full file


def test_delta_falls_back_to_full_file_when_month_file_is_missing(landing, monkeypatch):  # 404 -> full file
    delta_period(monkeypatch)                                   # delta mode
    monkeypatch.setattr(bp, "SOURCES", [ELHUB_DELTA])           # only the test source
    calls = []                                                  # URLs asked for
    monkeypatch.setattr(bp, "download", fake_source(calls))     # fake internet
    bp.run_bronze()                                             # first run: full file

    def month_missing(url, target, headers=None, attempts=3):   # the month file does not exist ...
        calls.append(url)                                       # remember the request
        if url.endswith("-00.csv"):                             # month file
            raise http_error(404)                               # ... 404
        fake_source([])(url, target)                            # full file as before
    monkeypatch.setattr(bp, "download", month_missing)          # use it
    calls.clear()                                               # second run
    result = bp.run_bronze()                                    # delta run
    assert result["status"] == "success" and result["warning"] == 1  # a warning, not a failure
    assert calls[-1] == "https://api.example/full.csv"          # the full file was downloaded instead
    assert bp.read_manifest()[-1]["status"] == "skipped_duplicate"  # same full file as before -> not stored twice


def test_delta_month_file_with_other_columns_is_not_used(landing, monkeypatch):  # other header -> full file
    delta_period(monkeypatch)                                   # delta mode
    monkeypatch.setattr(bp, "SOURCES", [ELHUB_DELTA])           # only the test source
    calls = []                                                  # URLs asked for
    monkeypatch.setattr(bp, "download", fake_source(calls, month_header="X;Y;Z"))  # month file has other columns
    bp.run_bronze()                                             # first run: full file
    result = bp.run_bronze()                                    # delta run
    assert result["warning"] == 1                               # month file rejected
    assert not [r for r in bp.read_manifest() if r["period"] == "2026-10" and r["status"] == "ingested"]  # never stored
    assert calls[-1] == "https://api.example/full.csv"          # full file instead


def test_frost_delta_requests_only_newest_months(landing, monkeypatch):  # Frost in delta mode
    delta_period(monkeypatch, start="2025-06", end="2025-10")   # five months, delta mode
    monkeypatch.setattr(bp, "frost_station_ids", lambda state: ["SN1"])  # one station -> one group
    state = {"manifest": [row("2025-09", dataset="frost_observations") | {"original_filename": "x.json"}]}  # Sep already there
    months = [m for _, m, _ in bp.frost_observation_requests(state, FROST_OBS)]  # months requested
    assert months == ["2025-09", "2025-10"]                     # from the high-water month, not the whole period


def test_wrong_load_mode_is_a_clear_error(landing, monkeypatch):  # typo in .env
    monkeypatch.setattr(bp, "LOAD_MODE", "detla")               # wrong value
    with pytest.raises(ValueError, match="NORGESPRIS_LOAD_MODE"):  # clear message ...
        bp.run_bronze()                                         # ... before anything is downloaded
