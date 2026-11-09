"""
Tests for bronze_pipeline.py  (run with: pytest -q tests/)

Tiny fake files in a temporary folder - no internet and no Spark needed.
They check the promises from section 3 of the project definition:
files are stored unchanged, metadata is written, duplicates are skipped,
schema changes are flagged, and failures are visible.
"""

import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import bronze_pipeline as bp  # noqa: E402

MANUAL = {"source": "met", "dataset": "test_temp", "mode": "manual", "sep": ";", "required": True}


@pytest.fixture
def landing(tmp_path, monkeypatch):
    """Point all folders to a temp dir and use one manual-only source."""
    monkeypatch.setattr(bp, "LANDING_DIR", str(tmp_path / "landing"))
    monkeypatch.setattr(bp, "BRONZE_DIR", str(tmp_path / "bronze"))
    monkeypatch.setattr(bp, "MANIFEST_PATH", str(tmp_path / "bronze" / "_manifest.csv"))
    monkeypatch.setattr(bp, "SOURCES", [MANUAL])
    folder = tmp_path / "landing" / "met" / "test_temp"
    folder.mkdir(parents=True)
    return folder


def test_file_is_stored_unchanged_with_metadata(landing):
    (landing / "a.csv").write_text("﻿Navn;Stasjon;Tid;Temp\r\nKjevik;SN39040;01.01.2024;-1,5\r\n",
                                   encoding="utf-8")
    result = bp.run_bronze()
    row = bp.read_manifest()[0]

    assert result["status"] == "success" and result["ingested"] == 1
    assert row["status"] == "ingested" and row["schema_status"] == "first_file"
    assert row["header"] == "Navn;Stasjon;Tid;Temp"                       # BOM/CRLF removed in metadata only
    stored = os.path.join(bp.BRONZE_DIR, row["bronze_path"])
    assert bp.sha256_of(stored) == bp.sha256_of(landing / "a.csv")        # byte-identical copy


def test_same_file_twice_is_skipped(landing):
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")
    bp.run_bronze()
    second = bp.run_bronze()
    assert second["ingested"] == 0 and second["skipped_duplicate"] == 1


def test_changed_header_is_flagged_but_kept(landing):
    (landing / "a.csv").write_text("A;B\n1;2\n", encoding="utf-8")
    bp.run_bronze()
    (landing / "b.csv").write_text("A;B;C\n1;2;3\n", encoding="utf-8")
    bp.run_bronze()
    row = [r for r in bp.read_manifest() if r["original_filename"] == "b.csv"][0]
    assert row["status"] == "ingested" and row["schema_status"] == "changed"


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


def test_frost_is_skipped_without_client_id(landing, monkeypatch):
    monkeypatch.delenv("FROST_CLIENT_ID", raising=False)
    assert bp.auth_headers({"auth": "frost"}) is None
    monkeypatch.setenv("FROST_CLIENT_ID", "abc")
    assert bp.auth_headers({"auth": "frost"})["Authorization"].startswith("Basic ")


def test_month_helpers():
    assert bp.month_list("2025-11", "2026-02") == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert bp.next_month("2025-12") == "2026-01"
    assert bp.last_complete_month(date(2026, 1, 15)) == "2025-12"


def test_months_to_download_skips_stored_but_rechecks_recent(monkeypatch):
    monkeypatch.setattr(bp, "ENV", "dev")
    monkeypatch.setattr(bp, "MONTH_RANGE", {"dev": ("2025-07", "2025-10")})
    cfg = {"dataset": "d"}
    state = {"manifest": [{"dataset": "d", "status": "ingested", "period": p}
                          for p in ["2025-07", "2025-09", "2025-10"]]}
    assert bp.months_to_download(state, cfg) == ["2025-08", "2025-09", "2025-10"]  # missing + 2 newest
    assert bp.months_to_download(state, dict(cfg, start="2025-10")) == ["2025-10"]  # dataset-specific start


def test_zip_is_kept_unpacked_and_checksummed_by_content(landing):
    import time
    import zipfile
    def make_zip(path):                                    # same CSV inside, but a new ZIP timestamp each time
        with zipfile.ZipFile(path, "w") as zf:
            info = zipfile.ZipInfo("part-00000-abc.csv", date_time=time.localtime()[:6])
            zf.writestr(info, "START_TIME,QUANTITY_KWH\n2025-10-01T00:00:00.000+02:00,1.5\n")
    make_zip(landing / "export.zip")
    bp.run_bronze()
    row = bp.read_manifest()[0]
    stored = os.path.join(bp.BRONZE_DIR, row["bronze_path"])

    assert stored.endswith(".zip")                                          # original kept as ZIP
    assert row["header"] == "START_TIME,QUANTITY_KWH"                       # header from the CSV inside
    assert os.path.exists(stored[:-4] + "_unzipped/part-00000-abc.csv")     # unpacked copy for Spark

    time.sleep(2)
    (landing / "export.zip").unlink()
    make_zip(landing / "export2.zip")                                       # new ZIP bytes, same data
    assert bp.run_bronze()["skipped_duplicate"] == 1                        # recognised as duplicate


def test_downloaded_csv_that_is_really_a_zip_gets_zip_name(landing, monkeypatch):
    import zipfile
    def zip_download(url, target, headers=None, attempts=3):
        with zipfile.ZipFile(target, "w") as zf:
            zf.writestr("part-00000.csv", "A;B\n1;2\n")
    monkeypatch.setattr(bp, "download", zip_download)
    monkeypatch.setattr(bp, "SOURCES", [{"source": "elhub", "dataset": "cons", "mode": "snapshot",
                                         "sep": ";", "required": True, "url": "https://x/cons.csv"}])
    bp.run_bronze()
    row = bp.read_manifest()[0]
    assert row["bronze_path"].endswith("cons.zip") and row["status"] == "ingested"


def test_gzip_download_is_unpacked(tmp_path):
    import gzip
    path = tmp_path / "f.csv"
    path.write_bytes(gzip.compress(b"A,B\n1,2\n"))         # what data.elhub.no sends
    bp.gunzip_if_needed(str(path))
    assert path.read_bytes() == b"A,B\n1,2\n"              # plain CSV again
    bp.gunzip_if_needed(str(path))                         # a plain file is left alone
    assert path.read_bytes() == b"A,B\n1,2\n"
