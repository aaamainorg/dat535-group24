"""
Tests for gold_pipeline.py  (run with: pytest -q tests/)

The whole star schema is built from a few hand-made Silver tables in a small
local Spark session - no PostgreSQL needed. The test data has a KNOWN answer:
private use (homes + cabins) is exactly 10 % higher after Norgespris than the
temperature predicts, business use does not change. Household and cabin are
split from private with Silver's distribution key (silver_pipeline.split_private). Each test checks one promise from the
Gold design in notebook 03 (section 1).
"""

import math                                                     # sine curve for the temperature
import os                                                       # paths
import sys                                                      # change the import path
from datetime import date, timedelta                            # dates in the test data
from decimal import Decimal                                     # exact decimal kWh values

import pytest                                                   # test framework
from pyspark.sql import SparkSession                            # local Spark for the tests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # repo root on the path
import gold_pipeline as gp  # noqa: E402
import silver_pipeline as sp  # noqa: E402

FIRST, LAST = date(2025, 1, 1), date(2026, 1, 5)        # 273 days before 1 Oct 2025, 97 days after
MUNIS = [("0301", "Oslo - Oslove", "03", "Oslo", "NO1", 700000), ("4601", "Bergen", "46", "Vestland", "NO5", 290000)]  # two municipalities: number, name, county, price area, population


@pytest.fixture(scope="module")                                 # one Spark session for this file
def spark():                                                    # test Spark session
    """A small local Spark session for the whole test file; gold_pipeline uses it through gp.spark."""
    session = (SparkSession.builder.master("local[1]").appName("test-gold")  # local, one core ...
               .config("spark.sql.shuffle.partitions", "2")     # ... few shuffle partitions ...
               .config("spark.ui.enabled", "false").getOrCreate())  # ... no web UI
    gp.spark = session                                          # gold_pipeline uses this session
    yield session                                               # give it to the tests
    session.stop()                                              # stop Spark after the last test


def temperature(i):                                             # test temperature of day i
    """A smooth, always-below-17 °C temperature series -> heating degree days are never 0."""
    return round(5 + 10 * math.sin(i / 20), 2)                  # between -5 and 15 °C


def silver_tables(spark):                                       # build the Silver test tables
    """Hand-made Silver tables with the real Silver columns (types from silver_pipeline.SILVER_SCHEMAS)."""
    def df(table, rows):                                        # helper: Silver-shaped DataFrame
        return spark.createDataFrame(rows, ", ".join(f"{n} {t}" for n, t in gp.SILVER_SCHEMAS[table]))  # columns from SILVER_SCHEMAS

    cons, temp, count, price_area = [], [], [], []              # rows of the four tables
    days = [FIRST + timedelta(days=i) for i in range((LAST - FIRST).days + 1)]  # every day of the test period
    for no, _, _, _, area, pop in MUNIS:                        # every municipality
        mp_p, mp_b = pop // 2, pop // 20                        # private and business meters
        for i, d in enumerate(days):                            # every day
            t = temperature(i)                                  # temperature that day
            hdd = 17 - t                                        # heating degree days
            factor = 1.10 if d >= gp.NORGESPRIS_START else 1.0      # the known answer: +10 % for private
            cons.append((no, d, "private", Decimal(str(round((10 + 2 * hdd) * factor * mp_p, 3))), mp_p, 24, True, True, None))  # private: 10 + 2 x HDD per meter
            cons.append((no, d, "business", Decimal(str(round((100 + 3 * hdd) * mp_b, 3))), mp_b, 24, True, True, None))  # business: 100 + 3 x HDD per meter
            temp.append((no, d, Decimal(str(t)), 1, "stations"))  # temperature row
            if d >= gp.NORGESPRIS_START:                            # Norgespris counts exist from the start only
                count.append((no, d, "household", mp_p // 5, mp_p * 3 // 4, Decimal("0.2667"), True))   # 75 % homes
                count.append((no, d, "cabin", mp_p // 20, mp_p // 4, Decimal("0.2"), True))  # 25 % cabin meters
    for d in days:                                              # every day:
        cons.append(("0000", d, "private", Decimal("123.456"), 5, 24, True, False, None))   # Elhub code 0: unknown
        for area in ("NO1", "NO5"):                                 # use per meter: household 40, cabin 20 (constant)
            price_area.append((d, area, "household", None, None, Decimal("0"), Decimal("0"), Decimal("4000"), 0, 0, 100))  # 4000 kWh / 100 meters
            price_area.append((d, area, "cabin", None, None, Decimal("0"), Decimal("0"), Decimal("200"), 0, 0, 10))  # 200 kWh / 10 meters
    price_area += [(date(2025, 10, 1), "NO1", "household", Decimal(lo) if lo else None, Decimal(hi) if hi else None,  # + two band rows on one day ...
                    Decimal("10"), Decimal("30"), Decimal("40"), 1, 3, 4)  # ... with these values ...
                   for lo, hi in (("0", "3000"), ("7000", "1000000000000"))]  # ... for these bands
    silver = {                                                  # the Silver tables:
        "municipality": df("municipality", [m[:5] for m in MUNIS]),  # municipalities
        "population_yearly": df("population_yearly", [(m[0], 2025, m[5]) for m in MUNIS]),   # no 2026 yet
        "consumption_daily": df("consumption_daily", cons),     # consumption
        "temperature_municipality_daily": df("temperature_municipality_daily", temp),  # temperature
        "norgespris_count_daily": df("norgespris_count_daily", count),  # Norgespris meter counts
        "norgespris_price_area_daily": df("norgespris_price_area_daily", price_area),  # Norgespris per price area
    }                                                           # end of the tables
    silver["private_split_daily"] = sp.split_private(silver["consumption_daily"], silver["municipality"],   # Silver's key
                                                     silver["norgespris_count_daily"], silver["norgespris_price_area_daily"])  # ... from the same inputs
    return silver                                               # all Silver test tables


@pytest.fixture(scope="module")                                 # build Gold once for all tests
def built(spark):                                               # Silver and Gold for the tests
    """(silver, gold) - the whole Gold layer built once for all tests below."""
    silver = silver_tables(spark)                               # Silver test tables
    return silver, gp.build_gold_tables(silver)                 # build all Gold tables


def test_dim_date_attributes(spark):                            # calendar attributes
    rows = {r.date: r for r in gp.build_dim_date(date(2025, 9, 29), date(2025, 10, 1)).collect()}  # three days around the start
    assert rows[date(2025, 9, 29)].date_key == 20250929 and rows[date(2025, 9, 29)].day_of_week == 1   # a Monday
    assert rows[date(2025, 9, 30)].norgespris_period == "before" and rows[date(2025, 10, 1)].norgespris_period == "after"  # before / after
    assert rows[date(2025, 10, 1)].days_since_norgespris == 0   # day 0 = start
    assert not rows[date(2025, 9, 30)].is_heating_season and rows[date(2025, 10, 1)].is_heating_season  # heating season from October
    assert rows[date(2025, 10, 1)].energy_year == "2025/26"     # July-June year


def test_dim_municipality_keys_price_area_and_unknown_row(spark):  # municipality dimension
    munis = spark.createDataFrame([("0301", "Oslo", "03", "Oslo", "NO1"), ("9999", "Somewhere", "99", "Unknown county", "unknown")],  # Oslo and an unknown county ...
                                  ", ".join(f"{n} {t}" for n, t in gp.SILVER_SCHEMAS["municipality"]))  # ... with the Silver columns
    got = {r.municipality_key: r for r in gp.build_dim_municipality(munis).collect()}  # key -> row
    assert got[301].price_area == "NO1" and got[301].price_area_key == 1 and got[301].county_name == "Oslo"  # Oslo: key 301, NO1
    assert got[9999].price_area == "unknown" and got[9999].price_area_key == -1      # county not in COUNTIES
    assert not got[-1].is_known and got[-1].municipality_name == "Unknown municipality"  # the unknown row


def test_customer_and_consumption_groups_are_separate_dimensions(spark):  # two group dimensions
    customers = spark.createDataFrame([("private",), ("business",), ("industry",), ("other",)], "customer_group string")  # customer groups + a new one
    got = {r.customer_group: r for r in gp.build_dim_customer_group(customers).collect()}  # group -> row
    assert got["private"].customer_group_key == 1 and got["private"].norgespris_eligible  # private: key 1, eligible
    assert not got["business"].norgespris_eligible and not got["industry"].norgespris_eligible  # business and industry: not eligible
    assert got["other"].customer_group_key == 100 and got["other"].norgespris_eligible is False   # new group
    norgespris = spark.createDataFrame([("household",), ("cabin",)], "consumption_group string")  # Norgespris groups
    got = {r.consumption_group: (r.consumption_group_key, r.norgespris_cap_kwh_month)  # group -> (key, cap) ...
           for r in gp.build_dim_consumption_group(norgespris).collect()}  # ... from the dimension
    assert got == {"household": (1, 5000), "cabin": (2, 1000)}  # fixed keys and caps


def test_no_kwh_lost_and_unknown_municipality_kept(built):      # no kWh lost
    silver, gold = built                                        # the test data
    silver_kwh = silver["consumption_daily"].agg({"kwh": "sum"}).first()[0]  # kWh in Silver
    fact = gold["fact_consumption_daily"]                       # the consumption fact
    assert fact.agg({"kwh": "sum"}).first()[0] == silver_kwh                     # check G-01 in miniature
    assert fact.filter("municipality_key = -1").count() == (LAST - FIRST).days + 1  # one unknown row per day


def test_grain_is_unique_and_no_orphan_keys(built):             # unique keys, no orphans
    _, gold = built                                             # the Gold tables
    for table, keys in gp.PRIMARY_KEYS.items():                 # every table and its key
        df = gold[table]                                        # the table
        assert df.count() == df.select(*keys).distinct().count(), table  # rows = distinct keys
    for table, column, dim in gp.FOREIGN_KEYS:                  # every foreign key
        orphans = gold[table].select(column).distinct().join(gold[dim].select(column), column, "left_anti").count()  # keys missing in the dimension
        assert orphans == 0, f"{table}.{column}"                # must be 0


def test_private_split_keeps_every_kwh_and_gets_norgespris_meters(built):  # split keeps kWh, Norgespris meters per group
    silver, gold = built                                        # the test data
    private = silver["consumption_daily"].filter("customer_group = 'private' AND known_municipality").agg({"kwh": "sum"}).first()[0]  # private kWh in Silver
    split = gold["fact_private_split_daily"]                    # the split fact
    assert split.agg({"kwh": "sum"}).first()[0] == private                       # household + cabin = private (G-11/G-12)
    rows = (split.join(gold["dim_consumption_group"], "consumption_group_key")  # the split rows ...
            .filter("municipality_key = 301 AND date_key = 20251002").collect())  # ... of Oslo on one day
    got = {r.consumption_group: (r.norgespris_metering_points, r.counted_metering_points) for r in rows}  # group -> (Norgespris meters, counted)
    assert got == {"household": (70000, 262500), "cabin": (17500, 87500)}       # household to household, cabin to cabin
    share = {r.consumption_group: r.share_of_private_kwh for r in rows}  # group -> share of private kWh
    assert share["household"] == pytest.approx(0.75 * 40 / (0.75 * 40 + 0.25 * 20), abs=1e-4)  # household share = key formula


def test_population_falls_back_to_newest_year(built):           # population fallback
    _, gold = built                                             # the test data
    row = gold["agg_municipality_daily"].filter("municipality_key = 301 AND date_key = 20260105").first()  # Oslo on a day in 2026
    assert row.population == 700000                                              # 2026 not published -> 2025


def test_temperature_adjusted_change_finds_the_known_answer(built):  # the temperature adjustment works
    _, gold = built                                             # the test data
    effect = {(r.level, r.area_code, r.measure): r for r in gold["agg_norgespris_effect"].collect()}  # (level, area, measure) -> row
    household = effect[("national", "NO", "kwh_per_household")]  # national household row
    private = effect[("national", "NO", "kwh_per_private_meter")]  # national private row
    business = effect[("national", "NO", "kwh_per_business_meter")]  # national business row
    assert private.adjusted_change_pct == pytest.approx(10.0, abs=0.1)           # +10 % built into the data (measured)
    assert household.adjusted_change_pct == pytest.approx(10.0, abs=0.1)         # same after the (constant) key
    assert business.adjusted_change_pct == pytest.approx(0.0, abs=0.1)            # no Norgespris -> no change
    assert household.adjusted_change_vs_business_pp == pytest.approx(10.0, abs=0.2)  # household minus business
    assert household.r2 > 0.99 and household.reliable           # good model, reliable
    assert abs(household.raw_change_pct - household.adjusted_change_pct) > 1      # raw before/after is misleading here
    assert effect[("municipality", "0301", "kwh_per_household")].adjusted_change_pct == pytest.approx(10.0, abs=0.1)  # also on municipality level


def test_consumption_bands_and_uptake_share(built):             # band labels and uptake shares
    _, gold = built                                             # the test data
    bands = {r.consumption_band for r in gold["fact_norgespris_price_area_daily"].collect()}  # band labels in the fact
    assert bands == {"0-3000", "7000+", "unknown"}                                # 10^12 = open upper limit
    uptake = gold["agg_norgespris_uptake_daily"].filter("date_key = 20251001 AND price_area_key = 1").first()  # uptake row: day 1, NO1
    assert uptake.metering_point_share == pytest.approx(70000 / 262500, abs=1e-4)   # household meters, Oslo
    assert uptake.kwh_share == 0.25                                               # 20 / (20 + 60)


def test_checks_cover_every_table():                            # the check list is complete
    ids = [c[0] for c in gp.gold_checks("gold_stage")]          # all check ids
    assert len(ids) == len(set(ids))                                              # every check id is unique
    assert set(gp.PRIMARY_KEYS) == set(gp.TABLES)                                 # every table has a grain check
    assert all(dim in gp.TABLES for _, _, dim in gp.FOREIGN_KEYS)  # every dimension is a Gold table
