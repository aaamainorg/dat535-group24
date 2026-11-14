"""
Tests for silver_pipeline.py  (run with: pytest -q tests/)

Every cleaning function is run on a few hand-made rows in a small local Spark
session - no PostgreSQL needed. Each test checks one promise from the Silver
design in notebook 02 (section 1): rejects, newest version per key, local
date, municipality matching, PT0H first, county average.
"""

import os
import sys
from datetime import date

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import silver_pipeline as sp  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """A small local Spark session for the whole test file (starting Spark takes a few seconds)."""
    session = (SparkSession.builder.master("local[1]").appName("test-silver")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def bronze_df(spark, columns, rows):
    """A Bronze-like DataFrame: all text columns + _landing_run_id."""
    return spark.createDataFrame(rows, columns + ["_landing_run_id"])


@pytest.fixture(scope="module")
def municipalities(spark):
    """A tiny silver.municipality: Oslo, Drammen (new code 3301), Halden, and two 'Herøy' (name not unique)."""
    rows = [("0301", "Oslo - Oslove", "03"), ("3301", "Drammen", "33"), ("3101", "Halden", "31"),
            ("1515", "Herøy", "15"), ("1818", "Herøy", "18"), ("5501", "Tromsø - Romsa - Tromssa", "55")]
    return spark.createDataFrame(rows, "municipality_no string, municipality_name string, county_no string")


def test_population_unpivot_codes_and_rejects(spark):
    df = bronze_df(spark, ["Kommune", "2023", "2024"], [
        ("K-3101 Halden", "31730", "31935", "r1"),
        ("K-23 Kontinentalsokkelen", "0", "0", "r1"),          # SSB-01: no 4-digit code
        ("K-0301 Oslo", "..", "717710", "r1"),                  # SSB-02 for 2023 ('..' = missing)
        ("K-3101 Halden", "31730", "32000", "r2"),              # newer file: 2024 value replaced
    ])
    population, municipality, rejects = sp.clean_population(df)
    got = {(r.municipality_no, r.year): r.population for r in population.collect()}
    assert got == {("3101", 2023): 31730, ("3101", 2024): 32000, ("0301", 2024): 717710}
    assert {r.rule_id for r in rejects.collect()} == {"SSB-01", "SSB-02"}
    names = {r.municipality_no: (r.municipality_name, r.county_no) for r in municipality.collect()}
    assert names == {"3101": ("Halden", "31"), "0301": ("Oslo", "03")}


CONS_COLS = ["START_TIME", "MUNICIPALITY_NUMBER", "NAME", "CONSUMPTION_GROUP", "QUANTITY_KWH",
             "METERING_POINT_COUNT", "LAST_UPDATE_TIME"]


def test_consumption_rejects_dedup_and_local_date(spark, municipalities):
    old, new = "2025-11-10T10:00:00.000+01:00", "2025-11-20T10:00:00.000+01:00"
    rows = [
        # 00:30 UTC on 25 Oct is 02:30 local -> local date 2025-10-25 (UTC would also say 25th, so add a late hour:)
        ("2025-10-25T23:00:00.000+02:00", "0301", "Oslo", "household", "10.000000", "5", old, "r1"),  # local 25th, UTC 25th 21:00
        ("2025-10-26T00:00:00.000+02:00", "0301", "Oslo", "household", "1.000000", "5", old, "r1"),   # local 26th, UTC 25th 22:00!
        ("2025-10-26T00:00:00.000+02:00", "301", "Oslo", "household", "2.000000", "5", new, "r1"),    # same hour, '301', newer -> wins
        ("2025-10-26T01:00:00.000+02:00", "4601", "Bergen", "household", "abc", "5", old, "r1"),      # CONS-02
        (None, "0301", "Oslo", "household", "1.0", "5", old, "r1"),                                   # CONS-01
        ("2025-10-26T02:00:00.000+02:00", "0301", "Oslo", "household", "-1.0", "5", old, "r1"),       # CONS-03
        ("2025-10-26T03:00:00.000+01:00", "0", "Ukjent", "business", "7.000000", "9", old, "r1"),     # unknown municipality
    ]
    daily, rejects, hourly = sp.clean_consumption(bronze_df(spark, CONS_COLS, rows), municipalities)
    got = {(r.municipality_no, r.date, r.consumption_group): (float(r.kwh), r.n_hours, r.known_municipality)
           for r in daily.collect()}
    assert got == {
        ("0301", date(2025, 10, 25), "household"): (10.0, 1, True),
        ("0301", date(2025, 10, 26), "household"): (2.0, 1, True),    # the newer '301' row replaced the old one
        ("0000", date(2025, 10, 26), "business"): (7.0, 1, False),    # kept, but marked unknown
    }
    assert sorted(r.rule_id for r in rejects.collect()) == ["CONS-01", "CONS-02", "CONS-03"]
    assert hourly.count() == 3                                         # 4 good hourly rows - 1 duplicate


def test_complete_day_allows_daylight_saving(spark, municipalities):
    # 26 Oct 2025: 00:00-02:59 summer time (+02:00), then the clock goes back: 02:00-23:00 winter time (+01:00)
    hours = [f"2025-10-26T{h:02d}:00:00.000+02:00" for h in range(3)] + \
            [f"2025-10-26T{h:02d}:00:00.000+01:00" for h in range(2, 24)]      # 3 + 22 = 25 different hours
    rows = [(t, "0301", "Oslo", "cabin", "1.0", "1", "2025-11-10T10:00:00.000+01:00", "r1") for t in hours]
    daily, _, _ = sp.clean_consumption(bronze_df(spark, CONS_COLS, rows), municipalities)
    row = daily.first()
    assert row.n_hours == 25 and row.complete_day is True


def test_station_municipality_by_code_or_unique_name(spark, municipalities):
    cols = ["id", "name", "municipalityId", "municipality", "geometry"]
    rows = [("SN1", "OSLO", "301", "OSLO", '{"coordinates":[10.7,59.9]}', "r1"),
            ("SN2", "DRAMMEN", "3005", "DRAMMEN", None, "r1"),        # old code -> matched by name
            ("SN3", "HERØY", "1534", "HERØY", None, "r1"),            # old code, name not unique -> no municipality
            ("SN4", "HEIDRUN", None, None, None, "r1"),               # at sea
            ("SN6", "TROMSØ", "1902", "TROMSØ", None, "r1"),          # old code, SSB name has Sami names too
            (None, "BROKEN", "301", "OSLO", None, "r1")]              # ST-01
    stations, rejects = sp.clean_stations(bronze_df(spark, cols, rows), municipalities)
    got = {r.station_id: (r.municipality_no, r.municipality_match, r.county_no) for r in stations.collect()}
    assert got == {"SN1": ("0301", "code", "03"), "SN2": ("3301", "name", "33"),
                   "SN3": (None, None, None), "SN4": (None, None, None), "SN6": ("5501", "name", "55")}
    assert [r.rule_id for r in rejects.collect()] == ["ST-01"]
    assert stations.filter("station_id = 'SN1'").first().latitude == 59.9


def test_temperature_pt0h_first_and_county_average(spark, municipalities):
    obs = ('[{"elementId":"mean(air_temperature P1D)","value":4.0,"timeOffset":"PT6H"},'
           '{"elementId":"mean(air_temperature P1D)","value":2.0,"timeOffset":"PT0H"}]')
    rows = [("SN1:0", "2025-10-26T00:00:00.000Z", obs, "r1"),
            ("SN2:0", "2025-10-26T00:00:00.000Z", '[{"elementId":"mean(air_temperature P1D)","value":99.0,"timeOffset":"PT0H"}]', "r1"),
            ("SN5:0", "2025-10-26T00:00:00.000Z", '[{"elementId":"mean(air_temperature P1D)","value":8.0,"timeOffset":"PT0H"}]', "r1")]
    temp, rejects = sp.clean_temperature(bronze_df(spark, ["sourceId", "referenceTime", "observations"], rows))
    got = {r.station_id: float(r.mean_temp_c) for r in temp.collect()}
    assert got == {"SN1": 2.0, "SN5": 8.0}                          # PT0H chosen; 99 °C rejected
    assert [r.rule_id for r in rejects.collect()] == ["TEMP-02"]

    stations = spark.createDataFrame([("SN1", "0301", "03"), ("SN5", "3301", "33")],
                                     "station_id string, municipality_no string, county_no string")
    muni = sp.temperature_per_municipality(temp, stations, municipalities)
    got = {r.municipality_no: (float(r.mean_temp_c), r.method) for r in muni.collect()}
    assert got["0301"] == (2.0, "stations")
    assert got["3301"] == (8.0, "stations")
    assert "3101" not in got                                         # county 31 has no station at all -> no row


def test_county_average_used_when_municipality_has_no_station(spark):
    munis = spark.createDataFrame([("3301", "Drammen", "33"), ("3303", "Kongsberg", "33")],
                                  "municipality_no string, municipality_name string, county_no string")
    temp = spark.createDataFrame([("SN5", date(2025, 10, 26), 8.0, "PT0H")],
                                 "station_id string, date date, mean_temp_c double, time_offset string")
    stations = spark.createDataFrame([("SN5", "3301", "33")], "station_id string, municipality_no string, county_no string")
    got = {r.municipality_no: (float(r.mean_temp_c), r.method)
           for r in sp.temperature_per_municipality(temp, stations, munis).collect()}
    assert got == {"3301": (8.0, "stations"), "3303": (8.0, "county_average")}


def test_norgespris_types_and_dedup(spark):
    cols = ["START_TIME", "PRICE_AREA", "CONSUMPTION_GROUP", "ESTIMATED_ANNUAL_CONSUMPTION_FROM",
            "ESTIMATED_ANNUAL_CONSUMPTION_TO", "QUANTITY_KWH_NORWAY_PRICE", "QUANTITY_KWH_NOT_NORWAY_PRICE",
            "NORGESPRIS_CONSUMPTION", "METERING_POINT_COUNT_NORWAY_PRICE", "METERING_POINT_COUNT_NOT_NORWAY_PRICE",
            "METERING_POINT_COUNT", "LAST_UPDATE_TIME"]
    base = ["2026-02-24T00:00:00.000+01:00", "NO1", "cabin", "0E-10", "3000.0000000000", "68512.566", "40212.309",
            "108724.875", "10702.0000000000", "14205.0000000000", "24907.0000000000"]
    rows = [tuple(base + ["2026-10-16T23:43:43.913+02:00", "r1"]),
            tuple(base[:5] + ["1.0", "1.0", "2.0"] + base[8:] + ["2026-10-17T00:00:00.000+02:00", "r1"]),  # newer
            tuple([base[0], ""] + base[2:] + ["2026-10-16T23:43:43.913+02:00", "r1"])]                    # NP-02
    df, rejects = sp.clean_norgespris(bronze_df(spark, cols, rows))
    row = df.first()
    assert df.count() == 1 and float(row.kwh_total) == 2.0           # the newer version won
    assert row.metering_points_total == 24907 and float(row.eac_from_kwh) == 0.0
    assert [r.rule_id for r in rejects.collect()] == ["NP-02"]


def test_conform_fails_on_missing_column(spark):
    with pytest.raises(Exception):
        sp.conform(spark.createDataFrame([("0301",)], "municipality_no string"), "municipality").collect()


def test_frost_date_is_norwegian_local_date(spark):
    """referenceTime is UTC; the date must be the Norwegian (Europe/Oslo, CET/CEST) date."""
    one = '[{"elementId":"mean(air_temperature P1D)","value":1.0,"timeOffset":"PT0H"}]'
    rows = [("SN1:0", "2025-10-25T00:00:00.000Z", one, "r1"),   # 02:00 Oslo (CEST) -> same day
            ("SN2:0", "2025-01-15T23:30:00.000Z", one, "r1"),   # 00:30 Oslo (CET, +1h)  -> next day
            ("SN3:0", "2025-07-15T22:30:00.000Z", one, "r1")]   # 00:30 Oslo (CEST, +2h) -> next day
    temp, _ = sp.clean_temperature(bronze_df(spark, ["sourceId", "referenceTime", "observations"], rows))
    got = {r.station_id: r.date for r in temp.collect()}
    assert got == {"SN1": date(2025, 10, 25), "SN2": date(2025, 1, 16), "SN3": date(2025, 7, 16)}


@pytest.mark.parametrize("count_col, muni_col", [("COUNT_NORWAYPRICE", "MUNICIPALITY_NUMBER"),
                                                  ("COUNT_NORWAY_PRICE", "MUNICIPALITY_NUM")])
def test_norgespris_count_groups_share_and_rejects(spark, municipalities, count_col, muni_col):
    cols = ["DATE", muni_col, "MUNICIPALITY", "CONSUMPTION_GROUP", count_col, "COUNT"]
    rows = [("2026-08-25", "301", "Oslo", "Household", "100", "400", "r1"),
            ("2026-08-25", "301", "Oslo", "Household", "120", "400", "r2"),               # newer file -> wins
            ("2026-08-25", "301", "Oslo", "Cabins and holiday properties", "0", "0", "r1"),  # share NULL (total 0)
            ("2026-08-25", "3005", "Drammen", "Household", "5", "10", "r1"),              # old code -> 3301 by name
            ("2026-08-25", "0", "Ukjent", "Household", "1", "2", "r1"),                   # unknown municipality, kept
            ("not a date", "301", "Oslo", "Household", "1", "2", "r1"),                   # NPC-01
            ("2026-08-25", "301", "Oslo", "Household", "9", "2", "r1")]                   # NPC-03: more than total
    df, rejects = sp.clean_norgespris_count(bronze_df(spark, cols, rows), municipalities)
    got = {(r.municipality_no, r.consumption_group): (r.metering_points_norgespris, r.metering_points_total,
                                                      None if r.norgespris_share is None else float(r.norgespris_share),
                                                      r.known_municipality) for r in df.collect()}
    assert got == {("0301", "household"): (120, 400, 0.3, True),
                   ("0301", "cabin"): (0, 0, None, True),
                   ("3301", "household"): (5, 10, 0.5, True),
                   ("0000", "household"): (1, 2, 0.5, False)}
    assert sorted(r.rule_id for r in rejects.collect()) == ["NPC-01", "NPC-03"]


def test_find_col_count_is_exact(spark):
    df = spark.createDataFrame([(1, 2)], "COUNT_NORWAYPRICE int, COUNT int")
    assert sp.find_col(df, "COUNT", exact=True) == "COUNT"                # not COUNT_NORWAYPRICE
    assert sp.find_col(df, "COUNT_NORWAY") == "COUNT_NORWAYPRICE"
    with pytest.raises(ValueError):
        sp.find_col(df, "MUNICIPALITY_NUM")


def test_norgespris_count_real_elhub_rows(spark, municipalities):
    """The first rows of the real Elhub file (header and values exactly as delivered)."""
    cols = ["DATE", "MUNICIPALITY_NUMBER", "MUNICIPALITY", "CONSUMPTION_GROUP", "COUNT_NORWAYPRICE", "COUNT"]
    rows = [("2025-10-01", "0", "Ukjent tilhørighet", "Household", "182", "1399", "r1"),
            ("2025-10-01", "0", "Ukjent tilhørighet", "Cabins and holiday properties", "0", "570", "r1"),
            ("2025-10-01", "301", "Oslo", "Household", "99977", "356525", "r1"),
            ("2025-10-01", "301", "Oslo", "Cabins and holiday properties", "0", "161", "r1")]
    df, rejects = sp.clean_norgespris_count(bronze_df(spark, cols, rows), municipalities)
    got = {(r.municipality_no, r.consumption_group): (r.metering_points_norgespris, r.metering_points_total,
                                                      float(r.norgespris_share), r.known_municipality)
           for r in df.collect()}
    assert got == {("0000", "household"): (182, 1399, 0.1301, False),      # unknown municipality: kept, marked
                   ("0000", "cabin"): (0, 570, 0.0, False),
                   ("0301", "household"): (99977, 356525, 0.2804, True),   # '301' -> '0301'
                   ("0301", "cabin"): (0, 161, 0.0, True)}
    assert rejects.count() == 0


def test_silver_period_from_env(monkeypatch):
    monkeypatch.setattr(sp, "month_range", lambda: ("2024-01", "2026-10"))
    assert sp.silver_period() == (date(2024, 1, 1), date(2026, 10, 31))     # whole months, both ends included
    monkeypatch.setattr(sp, "month_range", lambda: ("2024-01", None))      # empty end = last complete month
    monkeypatch.setattr(sp, "last_complete_month", lambda: "2024-02")
    assert sp.silver_period() == (date(2024, 1, 1), date(2024, 2, 29))      # leap year


def test_in_period_keeps_both_ends(spark):
    df = spark.createDataFrame([(date(2023, 12, 31),), (date(2024, 1, 1),), (date(2026, 10, 31),), (date(2026, 11, 1),)],
                               "date date")
    kept = sorted(r.date for r in sp.in_period(df, date(2024, 1, 1), date(2026, 10, 31)).collect())
    assert kept == [date(2024, 1, 1), date(2026, 10, 31)]


def test_population_codes_only_gives_empty_names(spark):
    """What SSB's API sends without 'usecodesandtexts' (codes only) - and what it sends with it."""
    codes_only = bronze_df(spark, ["region", "2024"], [("3101", "31935", "r1")])
    _, municipality, _ = sp.clean_population(codes_only)
    assert municipality.first().municipality_name == ""                  # -> metric municipalities_without_name
    with_names = bronze_df(spark, ["region", "2024"], [("3101 Halden", "31935", "r1")])
    _, municipality, _ = sp.clean_population(with_names)
    assert municipality.first().municipality_name == "Halden"


def test_population_ssb_api_layout(spark):
    """The layout SSB's API delivers with 'usecodesandtexts' (header and values as in the real file)."""
    cols = ["Region - region", "Personer1 - Personer 2023 - 2023", "Personer1 - Personer 2024 - 2024"]
    rows = [("K-3101 - K-3101 Halden", "31730", "31935", "r1"),
            ("K-3112 - K-3112 Råde", "8317", "7883", "r1"),
            ("K-0301 - K-0301 Oslo - Oslove", "709037", "717710", "r1"),
            ("K-23 - K-23 Kontinentalsokkelen", "0", "0", "r1")]          # SSB-01
    population, municipality, rejects = sp.clean_population(bronze_df(spark, cols, rows))
    names = {r.municipality_no: r.municipality_name for r in municipality.collect()}
    assert names == {"3101": "Halden", "3112": "Råde", "0301": "Oslo - Oslove"}
    assert {(r.municipality_no, r.year): r.population for r in population.collect()}[("3112", 2023)] == 8317
    assert population.count() == 6 and {r.rule_id for r in rejects.collect()} == {"SSB-01"}


def test_population_both_layouts_in_bronze(spark):
    """Manual export and API file in the same Bronze table (columns of the other layout are NULL)."""
    df = spark.createDataFrame(
        [("K-3101 Halden", "32000", None, None, "r1"), (None, None, "K-3101 - K-3101 Halden", "32038", "r2")],
        "Kommune string, `2025` string, `Region - region` string, `Personer1 - Personer 2025 - 2025` string, _landing_run_id string")
    population, municipality, rejects = sp.clean_population(df)
    assert [(r.municipality_no, r.year, r.population) for r in population.collect()] == [("3101", 2025, 32038)]  # newest file
    assert municipality.first().municipality_name == "Halden" and rejects.count() == 0
