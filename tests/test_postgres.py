"""
Tests for bronze_to_postgres.py  (run with: pytest -q tests/)

These only test the pure-Python parts (settings and fingerprint), so they need
neither a running PostgreSQL nor a Spark session. The real load is checked by
running the stage: python run_pipeline.py postgres
"""

import importlib                                                # reload a module with new settings
import os                                                       # paths and environment variables
import sys                                                      # change the import path

import pandas as pd                                             # the manifest is a pandas table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import bronze_to_postgres as pg  # noqa: E402


def reload_with(monkeypatch, **env):                            # helper: settings from the environment
    """Reload the module with the given environment variables (settings are read at import time)."""
    for key in ("NORGESPRIS_PG_DATABASE", "NORGESPRIS_PG_HOST", "NORGESPRIS_PG_PORT", "NORGESPRIS_PG_JDBC_JAR"):  # remove settings that would interfere ...
        monkeypatch.delenv(key, raising=False)                  # ... if they are set
    for key, value in env.items():                              # set the test values
        monkeypatch.setenv(key, value)                          # as environment variables
    return importlib.reload(pg)                                 # import the module again


def test_database_follows_environment(monkeypatch):             # database name follows dev/prod
    module = reload_with(monkeypatch, NORGESPRIS_PG_HOST="db.local", NORGESPRIS_PG_PORT="6543")  # custom host and port
    assert module.PG_URL == f"jdbc:postgresql://db.local:6543/norgespris_{module.ENV}"  # URL built from the settings
    assert module.PG_PROPERTIES["driver"] == "org.postgresql.Driver"  # the PostgreSQL JDBC driver
    assert "password" not in module.PG_URL                                # the password is never in the URL


def test_database_can_be_overridden(monkeypatch):               # database name can be set
    module = reload_with(monkeypatch, NORGESPRIS_PG_DATABASE="my_test_db")  # a test database name
    assert module.PG_URL.endswith("/my_test_db")                # it is used in the URL


def test_fingerprint_changes_only_when_bronze_files_change(monkeypatch, tmp_path):  # Bronze reloads only on new files
    module = reload_with(monkeypatch)                           # default settings
    monkeypatch.setattr(module, "LANDING_DIR", str(tmp_path))   # landing in a temp folder
    rows = [{"dataset": "d", "period": "snapshot", "original_filename": "d.csv", "status": "ingested",  # one stored file ...
             "run_id": "20261101T000000000000Z", "landing_path": "x/20261101T000000000000Z__d.csv"}]  # ... with run id and path
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))  # use it as the manifest
    first = module.bronze_fingerprint("d")                      # fingerprint of that file

    rows.append(dict(rows[0], status="skipped_duplicate", run_id="20261102T000000000000Z"))  # same file again
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))  # manifest with the duplicate
    assert module.bronze_fingerprint("d") == first                        # duplicate -> same file -> unchanged

    rows.append(dict(rows[0], run_id="20261103T000000000000Z",  # a new version of the file ...
                     landing_path="x/20261103T000000000000Z__d.csv"))      # a new version in Bronze
    monkeypatch.setattr(module, "manifest", pd.DataFrame(rows))  # manifest with the new version
    assert module.bronze_fingerprint("d") != first                        # -> reload needed
    assert module.bronze_fingerprint("not_there") is None                 # nothing in Bronze -> None


def test_missing_or_placeholder_password_fails_clearly(monkeypatch):  # no password -> clear failure
    for password in ("", "your-postgres-password"):             # empty and placeholder
        module = reload_with(monkeypatch, NORGESPRIS_PG_PASSWORD=password)  # settings with that password
        assert module.run_bronze_to_postgres()["status"] == "failed"     # stops before starting Spark


def test_delta_datasets_are_the_time_series():                  # which tables can be appended to
    module = importlib.import_module("bronze_to_postgres")      # the module as it is
    delta = {c["dataset"] for c in module.SOURCES if module.has_delta(c)}  # datasets loaded month by month
    assert delta == {"consumption_per_group_municipality_hour",            # Elhub hourly consumption
                     "norgespris_count_per_municipality_consumption_group",  # Norgespris meters per municipality
                     "frost_observations"}                                 # daily temperature


def test_files_since_full_load():                               # what is already in a bronze table
    log = pd.DataFrame([                                        # bronze._load_files as a small table:
        {"dataset": "d", "landing_file": "old.csv", "load_type": "full", "loaded_at": "2026-10-01T00:00:00+00:00"},   # old full load
        {"dataset": "d", "landing_file": "a.csv", "load_type": "full", "loaded_at": "2026-11-01T00:00:00+00:00"},     # newest full load
        {"dataset": "d", "landing_file": "m10.csv", "load_type": "delta", "loaded_at": "2026-11-06T00:00:00+00:00"},  # appended after it
        {"dataset": "e", "landing_file": "e.json", "load_type": "full", "loaded_at": "2026-11-01T00:00:00+00:00"},    # another dataset
    ])                                                          # end of the rows
    loaded = pg.files_since_full_load(log)                      # files per dataset
    assert loaded == {"d": {"a.csv", "m10.csv"}, "e": {"e.json"}}  # old.csv was replaced by the newest full load


def test_only_month_files_are_appended():                       # full files replace the table
    assert pg.is_month_file("/tmp/l/dev/elhub/d/2026-10/20261107T060000Z__d_2026-10.csv")  # a month file
    assert pg.is_month_file("/tmp/l/dev/elhub/d/2026-10/20261107T060000Z__d_2026-10_unzipped")  # its unpacked ZIP folder
    assert not pg.is_month_file("/tmp/l/dev/elhub/d/snapshot/20261107T060000Z__d.zip")  # a full file
    assert not pg.is_month_file("/tmp/l/dev/met/d/manual/20261107T060000Z__x.csv")  # a manual file
