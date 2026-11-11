"""
Tests for bronze_to_postgres.py  (run with: pytest -q tests/)

These only test the pure-Python parts (settings and fingerprint), so they need
neither a running PostgreSQL nor a Spark session. The real load is checked by
running the stage: python run_pipeline.py postgres
"""

import importlib
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import bronze_to_postgres as pg  # noqa: E402


def reload_with(monkeypatch, **env):
    """Reload the module with the given environment variables (settings are read at import time)."""
    for key in ("NORGESPRIS_PG_DATABASE", "NORGESPRIS_PG_HOST", "NORGESPRIS_PG_PORT", "NORGESPRIS_PG_JDBC_JAR"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(pg)


def test_database_follows_environment(monkeypatch):
    module = reload_with(monkeypatch, NORGESPRIS_PG_HOST="db.local", NORGESPRIS_PG_PORT="6543")
    assert module.PG_URL == f"jdbc:postgresql://db.local:6543/norgespris_{module.ENV}"
    assert module.PG_PROPERTIES["driver"] == "org.postgresql.Driver"
    assert "password" not in module.PG_URL                                # the password is never in the URL


def test_database_can_be_overridden(monkeypatch):
    module = reload_with(monkeypatch, NORGESPRIS_PG_DATABASE="my_test_db")
    assert module.PG_URL.endswith("/my_test_db")


def test_fingerprint_changes_only_when_bronze_files_change(monkeypatch, tmp_path):
    module = reload_with(monkeypatch)
    monkeypatch.setattr(module, "LANDING_DIR", str(tmp_path))
    rows = [{"dataset": "d", "period": "snapshot", "original_filename": "d.csv", "status": "ingested",
             "run_id": "20261101T000000000000Z", "landing_path": "x/20261101T000000000000Z__d.csv"}]
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))
    first = module.bronze_fingerprint("d")

    rows.append(dict(rows[0], status="skipped_duplicate", run_id="20261102T000000000000Z"))  # same file again
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))
    assert module.bronze_fingerprint("d") == first                        # duplicate -> same file -> unchanged

    rows.append(dict(rows[0], run_id="20261103T000000000000Z",
                     landing_path="x/20261103T000000000000Z__d.csv"))      # a new version in Bronze
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))
    assert module.bronze_fingerprint("d") != first                        # -> reload needed
    assert module.bronze_fingerprint("not_there") is None                 # nothing in Bronze -> None


def test_missing_or_placeholder_password_fails_clearly(monkeypatch):
    for password in ("", "your-postgres-password"):
        module = reload_with(monkeypatch, NORGESPRIS_PG_PASSWORD=password)
        assert module.run_bronze_to_postgres()["status"] == "failed"     # stops before starting Spark
