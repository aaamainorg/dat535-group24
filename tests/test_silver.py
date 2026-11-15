"""
Tests for silver_pipeline.py  (run with: pytest -q tests/)

Every cleaning function is run on a few hand-made rows in a small local Spark
session - no PostgreSQL needed. Each test checks one promise from the Silver
design in notebook 02 (section 1): rejects, newest version per key, local
date, municipality matching, PT0H first, county average.
"""

import os                                                       # paths
import sys                                                      # change the import path
from datetime import date                                       # dates in the test rows
from decimal import Decimal                                     # exact decimal kWh values

import pytest                                                   # test framework
from pyspark.sql import SparkSession                            # local Spark for the tests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import silver_pipeline as sp  # noqa: E402


@pytest.fixture(scope="module")                                 # one Spark session for this file
def spark():                                                    # test Spark session
    """A small local Spark session for the whole test file (starting Spark takes a few seconds)."""
    session = (SparkSession.builder.master("local[1]").appName("test-silver")  # local, one core ...
               .config("spark.sql.shuffle.partitions", "2")     # ... few shuffle partitions ...
               .config("spark.ui.enabled", "false").getOrCreate())  # ... no web UI
    yield session                                               # give it to the tests
    session.stop()                                              # stop Spark after the last test


def bronze_df(spark, columns, rows):                            # helper: fake Bronze table
    """A Bronze-like DataFrame: all text columns + _landing_run_id."""
    return spark.createDataFrame(rows, columns + ["_landing_run_id"])  # text columns + run id


@pytest.fixture(scope="module")                                 # shared test municipalities
def municipalities(spark):                                      # tiny silver.municipality
    """A tiny silver.municipality: Oslo, Drammen (new code 3301), Halden, and two 'Herøy' (name not unique)."""
    rows = [("0301", "Oslo - Oslove", "03"), ("3301", "Drammen", "33"), ("3101", "Halden", "31"),  # Oslo, Drammen, Halden ...
            ("1515", "Herøy", "15"), ("1818", "Herøy", "18"), ("5501", "Tromsø - Romsa - Tromssa", "55")]  # ... two Herøy, Tromsø
    return spark.createDataFrame(rows, "municipality_no string, municipality_name string, county_no string")  # as a DataFrame


def test_population_unpivot_codes_and_rejects(spark):           # SSB: wide -> long, rejects
    df = bronze_df(spark, ["Kommune", "2023", "2024"], [        # manual-export layout
        ("K-3101 Halden", "31730", "31935", "r1"),              # a normal row
        ("K-23 Kontinentalsokkelen", "0", "0", "r1"),          # SSB-01: no 4-digit code
        ("K-0301 Oslo", "..", "717710", "r1"),                  # SSB-02 for 2023 ('..' = missing)
        ("K-3101 Halden", "31730", "32000", "r2"),              # newer file: 2024 value replaced
    ])                                                          # end of the rows
    population, municipality, rejects = sp.clean_population(df)  # clean it
    got = {(r.municipality_no, r.year): r.population for r in population.collect()}  # (municipality, year) -> population
    assert got == {("3101", 2023): 31730, ("3101", 2024): 32000, ("0301", 2024): 717710}  # expected values
    assert {r.rule_id for r in rejects.collect()} == {"SSB-01", "SSB-02"}  # both rules were used
    names = {r.municipality_no: (r.municipality_name, r.county_no) for r in municipality.collect()}  # municipality -> (name, county)
    assert names == {"3101": ("Halden", "31"), "0301": ("Oslo", "03")}  # names after the code


CONS_COLS = ["START_TIME", "MUNICIPALITY_NUMBER", "NAME", "CONSUMPTION_GROUP", "QUANTITY_KWH",  # Bronze columns of the consumption file ...
             "METERING_POINT_COUNT", "LAST_UPDATE_TIME"]        # ... continued


def test_consumption_rejects_dedup_and_local_date(spark, municipalities):  # rejects, newest version, local date
    old, new = "2025-11-10T10:00:00.000+01:00", "2025-11-20T10:00:00.000+01:00"  # two update times
    rows = [                                                    # hourly test rows:
        # 00:30 UTC on 25 Oct is 02:30 local -> local date 2025-10-25 (UTC would also say 25th, so add a late hour:)
        ("2025-10-25T23:00:00.000+02:00", "0301", "Oslo", "household", "10.000000", "5", old, "r1"),  # local 25th, UTC 25th 21:00
        ("2025-10-26T00:00:00.000+02:00", "0301", "Oslo", "household", "1.000000", "5", old, "r1"),   # local 26th, UTC 25th 22:00!
        ("2025-10-26T00:00:00.000+02:00", "301", "Oslo", "household", "2.000000", "5", new, "r1"),    # same hour, '301', newer -> wins
        ("2025-10-26T01:00:00.000+02:00", "4601", "Bergen", "household", "abc", "5", old, "r1"),      # CONS-02
        (None, "0301", "Oslo", "household", "1.0", "5", old, "r1"),                                   # CONS-01
        ("2025-10-26T02:00:00.000+02:00", "0301", "Oslo", "household", "-1.0", "5", old, "r1"),       # CONS-03
        ("2025-10-26T03:00:00.000+01:00", "0", "Ukjent", "business", "7.000000", "9", old, "r1"),     # unknown municipality
    ]                                                           # end of the rows
    daily, rejects, hourly = sp.clean_consumption(bronze_df(spark, CONS_COLS, rows), municipalities)  # clean them
    got = {(r.municipality_no, r.date, r.customer_group): (float(r.kwh), r.n_hours, r.known_municipality)  # (municipality, date, group) -> values ...
           for r in daily.collect()}                            # ... for every daily row
    assert got == {                                             # expected daily rows:
        ("0301", date(2025, 10, 25), "household"): (10.0, 1, True),  # hour before midnight -> 25th
        ("0301", date(2025, 10, 26), "household"): (2.0, 1, True),    # the newer '301' row replaced the old one
        ("0000", date(2025, 10, 26), "business"): (7.0, 1, False),    # kept, but marked unknown
    }                                                           # end of the expected rows
    assert sorted(r.rule_id for r in rejects.collect()) == ["CONS-01", "CONS-02", "CONS-03"]  # the three rejects
    assert hourly.count() == 3                                         # 4 good hourly rows - 1 duplicate


def test_complete_day_allows_daylight_saving(spark, municipalities):  # 25-hour day counts as complete
    # 26 Oct 2025: 00:00-02:59 summer time (+02:00), then the clock goes back: 02:00-23:00 winter time (+01:00)
    hours = ([f"2025-10-26T{h:02d}:00:00.000+02:00" for h in range(3)]  # 3 summer-time hours ...
             + [f"2025-10-26T{h:02d}:00:00.000+01:00" for h in range(2, 24)])      # 3 + 22 = 25 different hours
    rows = [(t, "0301", "Oslo", "cabin", "1.0", "1", "2025-11-10T10:00:00.000+01:00", "r1") for t in hours]  # one row per hour
    daily, _, _ = sp.clean_consumption(bronze_df(spark, CONS_COLS, rows), municipalities)  # clean them
    row = daily.first()                                         # the one daily row
    assert row.n_hours == 25 and row.complete_day is True       # 25 hours, complete


def test_station_municipality_by_code_or_unique_name(spark, municipalities):  # station matching
    cols = ["id", "name", "municipalityId", "municipality", "geometry"]  # Bronze columns of the station file
    rows = [("SN1", "OSLO", "301", "OSLO", '{"coordinates":[10.7,59.9]}', "r1"),  # matched by code
            ("SN2", "DRAMMEN", "3005", "DRAMMEN", None, "r1"),        # old code -> matched by name
            ("SN3", "HERØY", "1534", "HERØY", None, "r1"),            # old code, name not unique -> no municipality
            ("SN4", "HEIDRUN", None, None, None, "r1"),               # at sea
            ("SN6", "TROMSØ", "1902", "TROMSØ", None, "r1"),          # old code, SSB name has Sami names too
            (None, "BROKEN", "301", "OSLO", None, "r1")]              # ST-01
    stations, rejects = sp.clean_stations(bronze_df(spark, cols, rows), municipalities)  # clean them
    got = {r.station_id: (r.municipality_no, r.municipality_match, r.county_no) for r in stations.collect()}  # station -> (municipality, how, county)
    assert got == {"SN1": ("0301", "code", "03"), "SN2": ("3301", "name", "33"),  # expected matches ...
                   "SN3": (None, None, None), "SN4": (None, None, None), "SN6": ("5501", "name", "55")}  # ... continued
    assert [r.rule_id for r in rejects.collect()] == ["ST-01"]  # the one reject
    assert stations.filter("station_id = 'SN1'").first().latitude == 59.9  # position read from JSON


def test_temperature_pt0h_first_and_county_average(spark, municipalities):  # PT0H series first, county average
    obs = ('[{"elementId":"mean(air_temperature P1D)","value":4.0,"timeOffset":"PT6H"},'  # two daily-mean series for SN1 ...
           '{"elementId":"mean(air_temperature P1D)","value":2.0,"timeOffset":"PT0H"}]')  # ... PT6H and PT0H
    rows = [("SN1:0", "2025-10-26T00:00:00.000Z", obs, "r1"),   # station SN1
            ("SN2:0", "2025-10-26T00:00:00.000Z", '[{"elementId":"mean(air_temperature P1D)","value":99.0,"timeOffset":"PT0H"}]', "r1"),  # an impossible 99 °C
            ("SN5:0", "2025-10-26T00:00:00.000Z", '[{"elementId":"mean(air_temperature P1D)","value":8.0,"timeOffset":"PT0H"}]', "r1")]  # station SN5
    temp, rejects = sp.clean_temperature(bronze_df(spark, ["sourceId", "referenceTime", "observations"], rows))  # clean them
    got = {r.station_id: float(r.mean_temp_c) for r in temp.collect()}  # station -> temperature
    assert got == {"SN1": 2.0, "SN5": 8.0}                          # PT0H chosen; 99 °C rejected
    assert [r.rule_id for r in rejects.collect()] == ["TEMP-02"]  # the one reject

    stations = spark.createDataFrame([("SN1", "0301", "03"), ("SN5", "3301", "33")],  # stations with municipality ...
                                     "station_id string, municipality_no string, county_no string")  # ... and county
    muni = sp.temperature_per_municipality(temp, stations, municipalities)  # temperature per municipality
    got = {r.municipality_no: (float(r.mean_temp_c), r.method) for r in muni.collect()}  # municipality -> (value, method)
    assert got["0301"] == (2.0, "stations")                     # own station
    assert got["3301"] == (8.0, "stations")                     # own station
    assert "3101" not in got                                         # county 31 has no station at all -> no row


def test_county_average_used_when_municipality_has_no_station(spark):  # county average used
    munis = spark.createDataFrame([("3301", "Drammen", "33"), ("3303", "Kongsberg", "33")],  # two municipalities in county 33 ...
                                  "municipality_no string, municipality_name string, county_no string")  # ... with these columns
    temp = spark.createDataFrame([("SN5", date(2025, 10, 26), 8.0, "PT0H")],  # one station temperature ...
                                 "station_id string, date date, mean_temp_c double, time_offset string")  # ... with these columns
    stations = spark.createDataFrame([("SN5", "3301", "33")], "station_id string, municipality_no string, county_no string")  # the station is in 3301
    got = {r.municipality_no: (float(r.mean_temp_c), r.method)  # municipality -> (value, method) ...
           for r in sp.temperature_per_municipality(temp, stations, munis).collect()}  # ... from the temperature function
    assert got == {"3301": (8.0, "stations"), "3303": (8.0, "county_average")}  # 3303 gets the county average


def test_norgespris_types_and_dedup(spark):                     # Norgespris price-area cleaning
    cols = ["START_TIME", "PRICE_AREA", "CONSUMPTION_GROUP", "ESTIMATED_ANNUAL_CONSUMPTION_FROM",  # Bronze columns ...
            "ESTIMATED_ANNUAL_CONSUMPTION_TO", "QUANTITY_KWH_NORWAY_PRICE", "QUANTITY_KWH_NOT_NORWAY_PRICE",  # ... continued
            "NORGESPRIS_CONSUMPTION", "METERING_POINT_COUNT_NORWAY_PRICE", "METERING_POINT_COUNT_NOT_NORWAY_PRICE",  # ... continued
            "METERING_POINT_COUNT", "LAST_UPDATE_TIME"]         # ... continued
    base = ["2026-02-24T00:00:00.000+01:00", "NO1", "cabin", "0E-10", "3000.0000000000", "68512.566", "40212.309",  # one real row from Elhub ...
            "108724.875", "10702.0000000000", "14205.0000000000", "24907.0000000000"]  # ... continued
    rows = [tuple(base + ["2026-10-16T23:43:43.913+02:00", "r1"]),  # the row
            tuple(base[:5] + ["1.0", "1.0", "2.0"] + base[8:] + ["2026-10-17T00:00:00.000+02:00", "r1"]),  # newer
            tuple([base[0], ""] + base[2:] + ["2026-10-16T23:43:43.913+02:00", "r1"])]                    # NP-02
    df, rejects = sp.clean_norgespris(bronze_df(spark, cols, rows))  # clean them
    row = df.first()                                            # the one result row
    assert df.count() == 1 and float(row.kwh_total) == 2.0           # the newer version won
    assert row.metering_points_total == 24907 and float(row.eac_from_kwh) == 0.0  # decimal text -> numbers
    assert [r.rule_id for r in rejects.collect()] == ["NP-02"]  # the one reject


def test_conform_fails_on_missing_column(spark):                # conform needs every column
    with pytest.raises(Exception):                              # must raise an error ...
        sp.conform(spark.createDataFrame([("0301",)], "municipality_no string"), "municipality").collect()  # ... because columns are missing


def test_frost_date_is_norwegian_local_date(spark):             # UTC -> Norwegian date
    """referenceTime is UTC; the date must be the Norwegian (Europe/Oslo, CET/CEST) date."""
    one = '[{"elementId":"mean(air_temperature P1D)","value":1.0,"timeOffset":"PT0H"}]'  # one observation
    rows = [("SN1:0", "2025-10-25T00:00:00.000Z", one, "r1"),   # 02:00 Oslo (CEST) -> same day
            ("SN2:0", "2025-01-15T23:30:00.000Z", one, "r1"),   # 00:30 Oslo (CET, +1h)  -> next day
            ("SN3:0", "2025-07-15T22:30:00.000Z", one, "r1")]   # 00:30 Oslo (CEST, +2h) -> next day
    temp, _ = sp.clean_temperature(bronze_df(spark, ["sourceId", "referenceTime", "observations"], rows))  # clean them
    got = {r.station_id: r.date for r in temp.collect()}        # station -> date
    assert got == {"SN1": date(2025, 10, 25), "SN2": date(2025, 1, 16), "SN3": date(2025, 7, 16)}  # Norwegian dates


@pytest.mark.parametrize("count_col, muni_col", [("COUNT_NORWAYPRICE", "MUNICIPALITY_NUMBER"),  # test both column-name variants ...
                                                  ("COUNT_NORWAY_PRICE", "MUNICIPALITY_NUM")])  # ... of the Elhub file
def test_norgespris_count_groups_share_and_rejects(spark, municipalities, count_col, muni_col):  # Norgespris count cleaning
    cols = ["DATE", muni_col, "MUNICIPALITY", "CONSUMPTION_GROUP", count_col, "COUNT"]  # Bronze columns
    rows = [("2026-08-25", "301", "Oslo", "Household", "100", "400", "r1"),  # older file
            ("2026-08-25", "301", "Oslo", "Household", "120", "400", "r2"),               # newer file -> wins
            ("2026-08-25", "301", "Oslo", "Cabins and holiday properties", "0", "0", "r1"),  # share NULL (total 0)
            ("2026-08-25", "3005", "Drammen", "Household", "5", "10", "r1"),              # old code -> 3301 by name
            ("2026-08-25", "0", "Ukjent", "Household", "1", "2", "r1"),                   # unknown municipality, kept
            ("not a date", "301", "Oslo", "Household", "1", "2", "r1"),                   # NPC-01
            ("2026-08-25", "301", "Oslo", "Household", "9", "2", "r1")]                   # NPC-03: more than total
    df, rejects = sp.clean_norgespris_count(bronze_df(spark, cols, rows), municipalities)  # clean them
    got = {(r.municipality_no, r.consumption_group): (r.metering_points_norgespris, r.metering_points_total,  # (municipality, group) -> counts ...
                                                      None if r.norgespris_share is None else float(r.norgespris_share),  # ... share ...
                                                      r.known_municipality) for r in df.collect()}  # ... known flag
    assert got == {("0301", "household"): (120, 400, 0.3, True),  # expected rows:
                   ("0301", "cabin"): (0, 0, None, True),       # cabin with 0 meters
                   ("3301", "household"): (5, 10, 0.5, True),   # old code mapped
                   ("0000", "household"): (1, 2, 0.5, False)}   # unknown kept
    assert sorted(r.rule_id for r in rejects.collect()) == ["NPC-01", "NPC-03"]  # the two rejects


def test_find_col_count_is_exact(spark):                        # COUNT is matched exactly
    df = spark.createDataFrame([(1, 2)], "COUNT_NORWAYPRICE int, COUNT int")  # two similar column names
    assert sp.find_col(df, "COUNT", exact=True) == "COUNT"                # not COUNT_NORWAYPRICE
    assert sp.find_col(df, "COUNT_NORWAY") == "COUNT_NORWAYPRICE"  # prefix match
    with pytest.raises(ValueError):                             # missing column ...
        sp.find_col(df, "MUNICIPALITY_NUM")                     # ... raises an error


def test_norgespris_count_real_elhub_rows(spark, municipalities):  # the real first rows of the Elhub file
    """The first rows of the real Elhub file (header and values exactly as delivered)."""
    cols = ["DATE", "MUNICIPALITY_NUMBER", "MUNICIPALITY", "CONSUMPTION_GROUP", "COUNT_NORWAYPRICE", "COUNT"]  # the real header
    rows = [("2025-10-01", "0", "Ukjent tilhørighet", "Household", "182", "1399", "r1"),  # unknown municipality, household
            ("2025-10-01", "0", "Ukjent tilhørighet", "Cabins and holiday properties", "0", "570", "r1"),  # unknown municipality, cabin
            ("2025-10-01", "301", "Oslo", "Household", "99977", "356525", "r1"),  # Oslo, household
            ("2025-10-01", "301", "Oslo", "Cabins and holiday properties", "0", "161", "r1")]  # Oslo, cabin
    df, rejects = sp.clean_norgespris_count(bronze_df(spark, cols, rows), municipalities)  # clean them
    got = {(r.municipality_no, r.consumption_group): (r.metering_points_norgespris, r.metering_points_total,  # (municipality, group) -> values ...
                                                      float(r.norgespris_share), r.known_municipality)  # ... share and known flag ...
           for r in df.collect()}                               # ... for every row
    assert got == {("0000", "household"): (182, 1399, 0.1301, False),      # unknown municipality: kept, marked
                   ("0000", "cabin"): (0, 570, 0.0, False),     # no Norgespris cabins
                   ("0301", "household"): (99977, 356525, 0.2804, True),   # '301' -> '0301'
                   ("0301", "cabin"): (0, 161, 0.0, True)}      # no Norgespris cabins
    assert rejects.count() == 0                                 # nothing rejected


def test_silver_period_from_env(monkeypatch):                   # period from the settings
    monkeypatch.setattr(sp, "month_range", lambda: ("2024-01", "2026-10"))  # fixed period
    assert sp.silver_period() == (date(2024, 1, 1), date(2026, 10, 31))     # whole months, both ends included
    monkeypatch.setattr(sp, "month_range", lambda: ("2024-01", None))      # empty end = last complete month
    monkeypatch.setattr(sp, "last_complete_month", lambda: "2024-02")  # fixed last complete month
    assert sp.silver_period() == (date(2024, 1, 1), date(2024, 2, 29))      # leap year


def test_in_period_keeps_both_ends(spark):                      # period filter
    df = spark.createDataFrame([(date(2023, 12, 31),), (date(2024, 1, 1),), (date(2026, 10, 31),), (date(2026, 11, 1),)],  # four dates around the period ...
                               "date date")                     # ... in one column
    kept = sorted(r.date for r in sp.in_period(df, date(2024, 1, 1), date(2026, 10, 31)).collect())  # dates kept
    assert kept == [date(2024, 1, 1), date(2026, 10, 31)]       # only the two inside


def test_population_codes_only_gives_empty_names(spark):        # SSB without names
    """What SSB's API sends without 'usecodesandtexts' (codes only) - and what it sends with it."""
    codes_only = bronze_df(spark, ["region", "2024"], [("3101", "31935", "r1")])  # codes only
    _, municipality, _ = sp.clean_population(codes_only)        # clean it
    assert municipality.first().municipality_name == ""                  # -> metric municipalities_without_name
    with_names = bronze_df(spark, ["region", "2024"], [("3101 Halden", "31935", "r1")])  # code and name
    _, municipality, _ = sp.clean_population(with_names)        # clean it
    assert municipality.first().municipality_name == "Halden"   # name found


def test_population_ssb_api_layout(spark):                      # SSB API layout
    """The layout SSB's API delivers with 'usecodesandtexts' (header and values as in the real file)."""
    cols = ["Region - region", "Personer1 - Personer 2023 - 2023", "Personer1 - Personer 2024 - 2024"]  # the real header
    rows = [("K-3101 - K-3101 Halden", "31730", "31935", "r1"),  # a normal row
            ("K-3112 - K-3112 Råde", "8317", "7883", "r1"),     # a Norwegian letter
            ("K-0301 - K-0301 Oslo - Oslove", "709037", "717710", "r1"),  # a Sámi name
            ("K-23 - K-23 Kontinentalsokkelen", "0", "0", "r1")]          # SSB-01
    population, municipality, rejects = sp.clean_population(bronze_df(spark, cols, rows))  # clean them
    names = {r.municipality_no: r.municipality_name for r in municipality.collect()}  # number -> name
    assert names == {"3101": "Halden", "3112": "Råde", "0301": "Oslo - Oslove"}  # names after the last code
    assert {(r.municipality_no, r.year): r.population for r in population.collect()}[("3112", 2023)] == 8317  # one value
    assert population.count() == 6 and {r.rule_id for r in rejects.collect()} == {"SSB-01"}  # 3 municipalities x 2 years, one reject


def test_population_both_layouts_in_bronze(spark):              # both layouts in one table
    """Manual export and API file in the same Bronze table (columns of the other layout are NULL)."""
    df = spark.createDataFrame(                                 # one row of each layout ...
        [("K-3101 Halden", "32000", None, None, "r1"), (None, None, "K-3101 - K-3101 Halden", "32038", "r2")],  # ... manual export and API
        "Kommune string, `2025` string, `Region - region` string, `Personer1 - Personer 2025 - 2025` string, _landing_run_id string")  # columns of both layouts
    population, municipality, rejects = sp.clean_population(df)  # clean them
    assert [(r.municipality_no, r.year, r.population) for r in population.collect()] == [("3101", 2025, 32038)]  # newest file
    assert municipality.first().municipality_name == "Halden" and rejects.count() == 0  # name and no rejects


def test_municipality_gets_county_name_and_price_area(spark):   # county name and price area
    munis = spark.createDataFrame([("4601", "Bergen", "46"), ("9901", "Somewhere", "99")],  # Bergen and an unknown county ...
                                  "municipality_no string, municipality_name string, county_no string")  # ... with these columns
    got = {r.municipality_no: (r.county_name, r.price_area) for r in sp.add_county(munis).collect()}  # number -> (county, price area)
    assert got == {"4601": ("Vestland", "NO5"), "9901": ("Unknown county", "unknown")}  # expected values


def split_inputs(spark, count_rows):                            # helper: inputs for the distribution key
    """Tiny Silver tables for the distribution key: Oslo (NO1), private 1 000 kWh / 100 meters on one day."""
    def df(table, rows):                                        # helper: Silver-shaped DataFrame
        return spark.createDataFrame(rows, ", ".join(f"{n} {t}" for n, t in sp.SILVER_SCHEMAS[table]))  # columns from SILVER_SCHEMAS
    d = date(2026, 1, 15)                                       # the test day
    consumption = df("consumption_daily", [                     # consumption on that day:
        ("0301", d, "private", Decimal("1000.000"), 100, 24, True, True, None),  # private
        ("0301", d, "business", Decimal("5000.000"), 10, 24, True, True, None)])        # not split
    municipality = df("municipality", [("0301", "Oslo", "03", "Oslo", "NO1")])  # Oslo in NO1
    count = df("norgespris_count_daily", count_rows)            # the given meter counts
    price_area = df("norgespris_price_area_daily", [                                    # per meter: household 40, cabin 20
        (d, "NO1", "household", None, None, Decimal("0"), Decimal("0"), Decimal("4000"), 0, 0, 100),  # 4000 kWh / 100 household meters
        (d, "NO1", "cabin", None, None, Decimal("0"), Decimal("0"), Decimal("200"), 0, 0, 10)])  # 200 kWh / 10 cabin meters
    return consumption, municipality, count, price_area         # the four inputs


def test_private_split_with_municipality_meter_mix(spark):      # split with the municipality's meter mix
    d = date(2026, 1, 15)                                       # the test day
    count_rows = [("0301", d, "household", 30, 75, Decimal("0.4"), True),                # 75 % household meters
                  ("0301", d, "cabin", 5, 25, Decimal("0.2"), True)]  # 25 % cabin meters
    row = sp.split_private(*split_inputs(spark, count_rows)).first()  # build the key
    assert row.household_meter_share == pytest.approx(0.75)     # 75 % household meters
    assert row.household_kwh_share == pytest.approx(0.75 * 40 / (0.75 * 40 + 0.25 * 20))  # = 0.857
    assert row.kwh_household + row.kwh_cabin == row.kwh_private == Decimal("1000.000")    # nothing lost
    assert (row.metering_points_household, row.metering_points_cabin) == (75, 25)  # meters split 75/25
    assert float(row.norgespris_share_household) == 0.4 and float(row.norgespris_share_private) == 0.35  # Norgespris shares
    assert row.key_method == "municipality"                     # how the key was found


def test_private_split_falls_back_to_price_area_mix(spark):     # fallback to the price area mix
    row = sp.split_private(*split_inputs(spark, [])).first()                            # no counts for Oslo
    assert row.household_meter_share == pytest.approx(100 / 110)                        # NO1: 100 household, 10 cabin meters
    assert row.key_method == "price_area" and row.norgespris_share_household is None  # how the key was found, no count
