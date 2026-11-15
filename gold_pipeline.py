#!/usr/bin/env python3
"""
DAT535 Group 24 - Gold layer: silver.* (PostgreSQL) -> gold.* (PostgreSQL)
===========================================================================

GENERATED from the code cells of notebooks/03_gold.ipynb (sections 2 and 4-7.1).
The notebook explains every line and the design choices; this file is what
GitHub Actions runs. If you change the logic, change the notebook cell and copy
it here, so the two stay identical.

Kimball star schema (dim_*, fact_*) + dashboard tables (agg_*), built in
gold_stage, checked against Silver, and only then swapped into gold (the
version before is kept in gold_previous). Every run is logged in gold.publish_log.

Usage:
    python run_pipeline.py gold        (recommended; after "silver")
    python gold_pipeline.py
"""

import logging                                                  # log messages (also visible in GitHub Actions)
import os                                                       # path of the run summary file
import time                                                     # elapsed time per step (performance evidence)
from datetime import date, datetime, timezone                   # run id, timestamps (UTC), the Norgespris start day
from functools import reduce                                    # combine several DataFrames into one

from pyspark import StorageLevel                                # keep reused DataFrames in memory (spill to disk if needed)
from pyspark.sql import functions as F                          # built-in column functions

from bronze_pipeline import ENV, LANDING_DIR                    # dev/prod + the landing folder (for the run summary file)
from bronze_to_postgres import get_spark, PG_URL, PG_PROPERTIES, PG_PASSWORD  # same Spark + database settings as Bronze/Silver
from silver_pipeline import SILVER_SCHEMAS                      # Silver column types (for an empty optional table)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold")                              # own logger name in the run log

spark = None                                                    # the SparkSession, set in run_gold() / the notebook
JDBC_READ = dict(PG_PROPERTIES, fetchsize="10000")              # stream 10 000 rows at a time (default: ALL rows into memory)

NORGESPRIS_START = date(2025, 10, 1)                            # first day with Norgespris (regjeringen.no)
HDD_BASE_C = 17.0                                               # heating degree days: base temperature 17 °C (Norwegian standard)
MIN_DAYS_BEFORE = 180                                           # a result is "reliable" with >= 180 days before the start ...
MIN_DAYS_AFTER = 28                                             # ... >= 28 days after the start ...
MIN_R2 = 0.3                                                    # ... and when temperature explains >= 30 % of the daily variation
STAGE, LIVE, PREVIOUS = "gold_stage", "gold", "gold_previous"   # build -> check -> publish; the version before is kept


PRICE_AREAS = {"NO1": "Øst-Norge", "NO2": "Sør-Norge", "NO3": "Midt-Norge", "NO4": "Nord-Norge", "NO5": "Vest-Norge"}
CUSTOMER_GROUPS = {                                             # silver.consumption_daily.customer_group
    "private": (1, "Private (homes + cabins)", True),           # -> (key, description, Norgespris eligible)
    "business": (2, "Business (næring)", False),
    "industry": (3, "Industry (industri)", False),
}
CONSUMPTION_GROUPS = {                                          # consumption_group in the two Norgespris tables
    "household": (1, "Household (bolig)", 5000),                # -> (key, description, cap kWh/month)
    "cabin": (2, "Cabin / holiday home (fritidsbolig)", 1000),
}
MEASURES = {                                                    # measure name -> what it means (shown in the dashboard)
    "kwh_per_private_meter": "kWh per private metering point per day (measured)",
    "kwh_per_household": "kWh per household metering point per day (allocated)",
    "kwh_household_per_capita": "household kWh per inhabitant per day (allocated)",
    "kwh_per_cabin": "kWh per cabin metering point per day (allocated)",
    "kwh_all_per_capita": "all kWh (private + business + industry) per inhabitant per day (measured)",
    "kwh_per_business_meter": "kWh per business metering point per day (control group, no Norgespris)",
}


def pg_query(sql):
    """Run a SELECT inside PostgreSQL and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=JDBC_READ)


def pg_execute(*statements):
    """Run statements that return nothing (CREATE/ALTER/DROP) in ONE transaction, via Spark's JDBC driver."""
    jvm = spark.sparkContext._jvm                               # the Java side of Spark, where the PostgreSQL driver is loaded
    props = jvm.java.util.Properties()                          # user + password for the connection
    props.setProperty("user", PG_PROPERTIES["user"])
    props.setProperty("password", PG_PROPERTIES["password"])
    conn = jvm.org.postgresql.Driver().connect(PG_URL, props)   # the same driver Spark uses to read and write
    try:
        conn.setAutoCommit(False)                               # start a transaction: all or nothing
        statement = conn.createStatement()
        for sql in statements:
            statement.execute(sql)
        conn.commit()                                           # make all changes visible at the same moment
    except Exception:
        conn.rollback()                                         # undo everything -> nothing half-done
        raise
    finally:
        conn.close()


def silver_tables():
    """Names of the tables in the silver schema."""
    rows = pg_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").collect()
    return {r["table_name"] for r in rows}


def read_silver(table, by_month=False):
    """Read silver.<table>; with by_month=True one Spark partition per month of the 'date' column."""
    if not by_month:                                            # small table -> one partition is fine
        return spark.read.jdbc(PG_URL, f"silver.{table}", properties=JDBC_READ)
    months = [r["m"] for r in pg_query(f"SELECT DISTINCT to_char(date, 'YYYY-MM-01') AS m "
                                       f"FROM silver.{table} WHERE date IS NOT NULL").collect()]
    predicates = [f"date >= DATE '{m}' AND date < DATE '{m}' + INTERVAL '1 month'" for m in sorted(months)]
    predicates.append("date IS NULL")                           # (Silver has none, but no row may ever be lost)
    return spark.read.jdbc(PG_URL, f"silver.{table}", predicates=predicates, properties=JDBC_READ)


def empty_silver(table):
    """An empty DataFrame with the columns of silver.<table>."""
    return spark.createDataFrame([], ", ".join(f"{name} {dtype}" for name, dtype in SILVER_SCHEMAS[table]))


def read_optional(table, available, by_month=False):
    """silver.<table> if it exists, otherwise an empty table with the same columns."""
    if table in available:
        return read_silver(table, by_month)
    logger.info(f"  silver.{table} does not exist - Gold continues without it (optional)")
    return empty_silver(table)


def write_stage(df, table):
    """Replace gold_stage.<table> with df; returns the number of rows now in PostgreSQL."""
    (df.write
       .option("batchsize", 10000)                              # rows per INSERT batch
       .jdbc(PG_URL, f"{STAGE}.{table}", mode="overwrite", properties=PG_PROPERTIES))
    rows = pg_query(f"SELECT COUNT(*) AS n FROM {STAGE}.{table}").first()["n"]  # count inside the database
    logger.info(f"  {STAGE}.{table}: {rows:,} rows")
    return rows


def date_key(column):
    """SQL for the integer date key yyyymmdd of a date column (2025-10-01 -> 20251001)."""
    return f"CAST(date_format({column}, 'yyyyMMdd') AS INT)"


def build_dim_date(first_day, last_day):
    """One row per day from first_day to last_day, with calendar and Norgespris attributes."""
    return spark.sql(f"""
        SELECT {date_key('d')}                                             AS date_key,
               d                                                           AS date,
               year(d)                                                     AS year,
               quarter(d)                                                  AS quarter,
               month(d)                                                    AS month,
               date_format(d, 'MMMM')                                      AS month_name,
               date_format(d, 'yyyy-MM')                                   AS year_month,
               weekofyear(d)                                               AS iso_week,
               weekday(d) + 1                                              AS day_of_week,     -- 1 = Monday
               date_format(d, 'EEEE')                                      AS day_name,
               weekday(d) >= 5                                             AS is_weekend,
               CASE WHEN month(d) IN (12, 1, 2) THEN 'winter' WHEN month(d) IN (3, 4, 5) THEN 'spring'
                    WHEN month(d) IN (6, 7, 8) THEN 'summer' ELSE 'autumn' END   AS season,
               month(d) IN (10, 11, 12, 1, 2, 3, 4)                        AS is_heating_season,
               CASE WHEN month(d) >= 7                                      -- July-June, e.g. '2025/26'
                    THEN concat(CAST(year(d) AS STRING), '/', substr(CAST(year(d) + 1 AS STRING), 3, 2))
                    ELSE concat(CAST(year(d) - 1 AS STRING), '/', substr(CAST(year(d) AS STRING), 3, 2)) END AS energy_year,
               CASE WHEN d >= DATE '{NORGESPRIS_START}' THEN 'after' ELSE 'before' END AS norgespris_period,
               datediff(d, DATE '{NORGESPRIS_START}')                      AS days_since_norgespris
        FROM (SELECT explode(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL 1 DAY)) AS d)
    """)


def price_area_key(column):
    """'NO3' -> 3, anything else -> -1 (unknown)."""
    return F.when(column.rlike("^NO[0-9]$"), F.substring(column, 3, 1).cast("int")).otherwise(F.lit(-1))


def build_dim_municipality(municipality):
    """silver.municipality (with county name + main price area from Silver), plus one 'unknown' row (key -1)."""
    known = (municipality
             .select(F.expr("try_cast(municipality_no AS INT)").alias("municipality_key"),  # '0301' -> 301 (stable)
                     "municipality_no", "municipality_name", "county_no",
                     F.coalesce("county_name", F.lit("Unknown county")).alias("county_name"),
                     F.coalesce("price_area", F.lit("unknown")).alias("price_area"),
                     F.lit(True).alias("is_known")))
    unknown = spark.createDataFrame([(-1, "unknown", "Unknown municipality", "00", "Unknown county", "unknown", False)],
                                    known.schema)              # for Elhub code 0 and codes SSB does not know
    dim = known.unionByName(unknown)
    return dim.withColumn("price_area_key", price_area_key(F.col("price_area")))


def build_dim_price_area(norgespris):
    """NO1-NO5 + unknown, plus any other code found in the Norgespris price-area data."""
    rows = list(PRICE_AREAS.items()) + [("unknown", "Unknown price area")]
    static = spark.createDataFrame(rows, "price_area string, price_area_name string")
    extra = (norgespris.select("price_area").where("price_area IS NOT NULL").distinct()
             .join(static, "price_area", "left_anti")         # codes we did not expect
             .withColumn("price_area_name", F.col("price_area")))
    dim = static.unionByName(extra)
    return dim.select(price_area_key(F.col("price_area")).alias("price_area_key"), "price_area", "price_area_name")


def add_new_groups(known, groups, column):
    """known dimension rows + one row (key 100, 101, ...) for every value of `column` in the data that is not known."""
    known_names = {r[column] for r in known.select(column).collect()}            # 2-3 names
    found = groups.select(column).where(f"{column} IS NOT NULL").distinct().collect()  # the few names in the data
    new_names = sorted(r[column] for r in found if r[column] not in known_names)  # values we did not expect
    if not new_names:
        return known
    new = spark.createDataFrame([(100 + i, name, name) for i, name in enumerate(new_names)],   # numbered in Python:
                                f"{column}_key int, {column} string, description string")     # a handful of rows
    return known.unionByName(new, allowMissingColumns=True)     # the other columns stay NULL for a new group


def build_dim_customer_group(groups):
    """Customer groups of silver.consumption_daily: private/business/industry with fixed keys, new ones from 100."""
    known = spark.createDataFrame(
        [(key, name, text, eligible) for name, (key, text, eligible) in CUSTOMER_GROUPS.items()],
        "customer_group_key int, customer_group string, description string, norgespris_eligible boolean")
    dim = add_new_groups(known, groups, "customer_group")
    return dim.withColumn("norgespris_eligible", F.coalesce("norgespris_eligible", F.lit(False)))  # new -> not eligible


def build_dim_consumption_group(groups):
    """Norgespris consumption groups of the two Norgespris tables: household/cabin with fixed keys, new ones from 100."""
    known = spark.createDataFrame(
        [(key, name, text, cap) for name, (key, text, cap) in CONSUMPTION_GROUPS.items()],
        "consumption_group_key int, consumption_group string, description string, norgespris_cap_kwh_month int")
    return add_new_groups(known, groups, "consumption_group")


def build_fact_consumption_daily():
    """municipality x day x customer group: kWh and meter counts with dimension keys (unknown municipality -> -1)."""
    return spark.sql(f"""
        SELECT /*+ BROADCAST(m, g) */
               {date_key('c.date')}                         AS date_key,
               COALESCE(m.municipality_key, -1)             AS municipality_key,
               g.customer_group_key,
               COALESCE(m.price_area_key, -1)               AS price_area_key,
               CAST(SUM(c.kwh) AS DECIMAL(18,3))            AS kwh,
               CAST(SUM(c.metering_points) AS INT)          AS metering_points,
               BOOL_AND(c.complete_day)                     AS complete_day
        FROM silver_consumption_daily c
        JOIN dim_customer_group g ON g.customer_group = c.customer_group
        LEFT JOIN dim_municipality m ON m.municipality_no = c.municipality_no AND c.known_municipality
        GROUP BY 1, 2, 3, 4
    """)


def build_fact_private_split_daily():
    """municipality x day x consumption group (household/cabin): private kWh and meters allocated by Silver's key."""
    return spark.sql(f"""
        WITH long AS (                                 -- household and cabin columns -> one row per consumption group
            SELECT s.municipality_no, s.date, s.key_method, x.*
            FROM silver_private_split_daily s
            LATERAL VIEW stack(2,
                'household', s.kwh_household, s.metering_points_household, s.norgespris_meters_household,
                             s.counted_meters_household, s.household_kwh_share,
                'cabin',     s.kwh_cabin, s.metering_points_cabin, s.norgespris_meters_cabin,
                             s.counted_meters_cabin, 1 - s.household_kwh_share)
                x AS consumption_group, kwh, metering_points, norgespris_metering_points, counted_metering_points,
                     share_of_private_kwh
            WHERE s.household_kwh_share IS NOT NULL)        -- no key -> nothing to allocate
        SELECT /*+ BROADCAST(m, g) */
               {date_key('l.date')}                         AS date_key,
               m.municipality_key,
               g.consumption_group_key,
               m.price_area_key,
               l.kwh, l.metering_points, l.norgespris_metering_points, l.counted_metering_points,
               ROUND(l.share_of_private_kwh, 4)             AS share_of_private_kwh,
               l.key_method
        FROM long l
        JOIN dim_municipality m ON m.municipality_no = l.municipality_no
        JOIN dim_consumption_group g ON g.consumption_group = l.consumption_group
    """)


def build_fact_weather_daily():
    """municipality x day: temperature and heating degree days."""
    return spark.sql(f"""
        SELECT /*+ BROADCAST(m) */
               {date_key('t.date')}                         AS date_key,
               m.municipality_key, m.price_area_key,
               t.mean_temp_c,
               CAST(GREATEST({HDD_BASE_C} - t.mean_temp_c, 0) AS DECIMAL(5,2)) AS hdd,   -- heating degree days
               t.n_stations,
               t.method                                     AS temperature_method
        FROM silver_temperature_municipality_daily t
        JOIN dim_municipality m ON m.municipality_no = t.municipality_no
        WHERE t.mean_temp_c IS NOT NULL
    """)


def build_fact_population_yearly():
    """municipality x year: population on 1 January."""
    return spark.sql("""
        SELECT /*+ BROADCAST(m) */
               p.year * 10000 + 101 AS date_key,           -- 1 January of that year
               m.municipality_key, m.price_area_key, p.year, p.population
        FROM silver_population_yearly p
        JOIN dim_municipality m ON m.municipality_no = p.municipality_no
    """)


def build_fact_norgespris_price_area_daily():
    """day x price area x consumption group x consumption band: kWh and meters with / without Norgespris."""
    def limit(c):                                               # 3000.000 -> '3000', NULL -> '?'
        return f"COALESCE(CAST(CAST({c} AS BIGINT) AS STRING), '?')"
    return spark.sql(f"""
        SELECT /*+ BROADCAST(a, g) */
               {date_key('n.date')}                         AS date_key,
               COALESCE(a.price_area_key, -1)               AS price_area_key,
               g.consumption_group_key,
               CASE WHEN n.eac_from_kwh IS NULL AND n.eac_to_kwh IS NULL THEN 'unknown'
                    WHEN n.eac_to_kwh IS NULL OR n.eac_to_kwh >= 1e9 THEN concat({limit('n.eac_from_kwh')}, '+')
                    ELSE concat({limit('n.eac_from_kwh')}, '-', {limit('n.eac_to_kwh')}) END AS consumption_band,
               n.eac_from_kwh, n.eac_to_kwh,
               n.kwh_norgespris, n.kwh_not_norgespris, n.kwh_total,
               n.metering_points_norgespris, n.metering_points_not_norgespris, n.metering_points_total
        FROM silver_norgespris_price_area_daily n
        JOIN dim_consumption_group g ON g.consumption_group = n.consumption_group
        LEFT JOIN dim_price_area a ON a.price_area = n.price_area
    """)


def build_agg_municipality_daily():
    """municipality x day: kWh per customer group, population, temperature and the ratios the dashboard needs."""
    return spark.sql("""
        WITH c AS (                                    -- measured: customer groups side by side
            SELECT f.date_key, f.municipality_key,
                   SUM(CASE WHEN g.customer_group = 'private' THEN f.kwh END)                 AS kwh_private,
                   SUM(CASE WHEN g.customer_group = 'business' THEN f.kwh END)                AS kwh_business,
                   SUM(CASE WHEN g.customer_group = 'industry' THEN f.kwh END)                AS kwh_industry,
                   SUM(f.kwh)                                                                 AS kwh_all,
                   SUM(CASE WHEN g.customer_group = 'private' THEN f.metering_points END)     AS metering_points_private,
                   SUM(CASE WHEN g.customer_group = 'business' THEN f.metering_points END)    AS metering_points_business,
                   SUM(CASE WHEN g.customer_group = 'industry' THEN f.metering_points END)    AS metering_points_industry,
                   BOOL_AND(f.complete_day)                                                   AS complete_day
            FROM fact_consumption_daily f
            JOIN dim_customer_group g ON g.customer_group_key = f.customer_group_key
            WHERE f.municipality_key <> -1
            GROUP BY f.date_key, f.municipality_key),
        h AS (                                         -- allocated: household and cabin side by side
            SELECT f.date_key, f.municipality_key,
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.kwh END)                        AS kwh_household,
                   SUM(CASE WHEN g.consumption_group = 'cabin' THEN f.kwh END)                            AS kwh_cabin,
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.metering_points END)            AS metering_points_household,
                   SUM(CASE WHEN g.consumption_group = 'cabin' THEN f.metering_points END)                AS metering_points_cabin,
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.norgespris_metering_points END) AS norgespris_household,
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.counted_metering_points END)    AS counted_household
            FROM fact_private_split_daily f
            JOIN dim_consumption_group g ON g.consumption_group_key = f.consumption_group_key
            GROUP BY f.date_key, f.municipality_key),
        pop AS (                                       -- population for every year, from the newest year <= that year
            SELECT y.year, p.municipality_key, MAX_BY(p.population, p.year) AS population
            FROM (SELECT DISTINCT year FROM dim_date) y
            JOIN fact_population_yearly p ON p.year <= y.year
            GROUP BY y.year, p.municipality_key)
        SELECT c.municipality_key, c.date_key, d.date, d.norgespris_period,
               c.kwh_private, c.kwh_business, c.kwh_industry, c.kwh_all,
               c.metering_points_private, c.metering_points_business, c.metering_points_industry,
               h.kwh_household, h.kwh_cabin, h.metering_points_household, h.metering_points_cabin,
               p.population, w.mean_temp_c, w.hdd,
               ROUND(try_divide(CAST(c.kwh_private AS DOUBLE), c.metering_points_private), 4)     AS kwh_per_private_meter,
               ROUND(try_divide(CAST(h.kwh_household AS DOUBLE), h.metering_points_household), 4) AS kwh_per_household,
               ROUND(try_divide(CAST(h.kwh_household AS DOUBLE), p.population), 4)                AS kwh_household_per_capita,
               ROUND(try_divide(CAST(h.kwh_cabin AS DOUBLE), h.metering_points_cabin), 4)         AS kwh_per_cabin,
               ROUND(try_divide(CAST(c.kwh_all AS DOUBLE), p.population), 4)                      AS kwh_all_per_capita,
               ROUND(try_divide(CAST(c.kwh_business AS DOUBLE), c.metering_points_business), 4)   AS kwh_per_business_meter,
               h.norgespris_household, h.counted_household,
               ROUND(try_divide(CAST(h.norgespris_household AS DOUBLE), h.counted_household), 4)  AS norgespris_share_household,
               c.complete_day
        FROM c
        JOIN dim_date d ON d.date_key = c.date_key
        LEFT JOIN h ON h.date_key = c.date_key AND h.municipality_key = c.municipality_key
        LEFT JOIN fact_weather_daily w ON w.date_key = c.date_key AND w.municipality_key = c.municipality_key
        LEFT JOIN pop p ON p.municipality_key = c.municipality_key AND p.year = d.year
    """)


def keep(df, name):
    """Compute df now, keep the result and register it as view `name` (see 5.5: why localCheckpoint, not persist)."""
    df = df.localCheckpoint()                                   # materialise + cut the lineage (the plan becomes one scan)
    df.createOrReplaceTempView(name)
    return df


def build_area_views():
    """Register v_area, v_long, v_model and v_daily (see the markdown above); v_model and v_daily are materialised."""
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW v_base AS
        SELECT a.*, m.municipality_no, m.municipality_name, m.county_no, m.county_name, m.price_area, p.price_area_name
        FROM agg_municipality_daily a
        JOIN dim_municipality m ON m.municipality_key = a.municipality_key
        LEFT JOIN dim_price_area p ON p.price_area_key = m.price_area_key
        WHERE a.complete_day AND a.hdd IS NOT NULL AND a.population IS NOT NULL
    """)
    sums = """SUM(kwh_private) AS kwh_private, SUM(kwh_household) AS kwh_household, SUM(kwh_cabin) AS kwh_cabin,
              SUM(kwh_business) AS kwh_business, SUM(kwh_all) AS kwh_all,
              SUM(metering_points_private) AS mp_private, SUM(metering_points_household) AS mp_household,
              SUM(metering_points_cabin) AS mp_cabin, SUM(metering_points_business) AS mp_business,
              SUM(population) AS population,
              SUM(norgespris_household) AS np_household, SUM(counted_household) AS counted_household,
              SUM(CAST(hdd AS DOUBLE) * population) / SUM(population)         AS hdd,          -- population-weighted
              SUM(CAST(mean_temp_c AS DOUBLE) * population) / SUM(population) AS mean_temp_c"""
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_area AS
        SELECT CASE WHEN grouping(county_no) = 0 THEN 'county'
                    WHEN grouping(price_area) = 0 THEN 'price_area' ELSE 'national' END     AS level,
               CASE WHEN grouping(county_no) = 0 THEN county_no
                    WHEN grouping(price_area) = 0 THEN price_area ELSE 'NO' END             AS area_code,
               CASE WHEN grouping(county_no) = 0 THEN MAX(county_name)
                    WHEN grouping(price_area) = 0 THEN MAX(price_area_name) ELSE 'Norway' END AS area_name,
               date_key, {sums}
        FROM v_base
        GROUP BY date_key, GROUPING SETS ((county_no), (price_area), ())
        UNION ALL
        SELECT 'municipality', municipality_no, municipality_name, date_key, {sums}
        FROM v_base
        GROUP BY municipality_no, municipality_name, date_key
    """)
    spark.sql("""
        SELECT level, area_code, area_name, date_key, hdd, mean_temp_c,
               try_divide(CAST(np_household AS DOUBLE), counted_household) AS norgespris_share_household,
               measure, value
        FROM v_area
        LATERAL VIEW stack(6,
            'kwh_per_private_meter',    try_divide(CAST(kwh_private AS DOUBLE), mp_private),
            'kwh_per_household',        try_divide(CAST(kwh_household AS DOUBLE), mp_household),
            'kwh_household_per_capita', try_divide(CAST(kwh_household AS DOUBLE), population),
            'kwh_all_per_capita',       try_divide(CAST(kwh_all AS DOUBLE), population),
            'kwh_per_cabin',            try_divide(CAST(kwh_cabin AS DOUBLE), mp_cabin),
            'kwh_per_business_meter',   try_divide(CAST(kwh_business AS DOUBLE), mp_business)) s AS measure, value
        WHERE value IS NOT NULL
    """).createOrReplaceTempView("v_long")                    # not kept: read twice, from the kept agg_municipality_daily
    keep(spark.sql("""
        SELECT l.level, l.area_code, l.measure,
               regr_intercept(l.value, l.hdd) AS intercept,      -- a: use on a day without heating
               regr_slope(l.value, l.hdd)     AS slope,          -- b: extra use per heating degree day
               regr_r2(l.value, l.hdd)        AS r2,             -- how much of the variation temperature explains
               COUNT(*)                       AS days_before,
               AVG(l.value)                   AS avg_before,
               AVG(l.mean_temp_c)             AS mean_temp_before
        FROM v_long l JOIN dim_date d ON d.date_key = l.date_key
        WHERE d.norgespris_period = 'before'
        GROUP BY l.level, l.area_code, l.measure
    """), "v_model")
    keep(spark.sql("""
        SELECT l.*, d.date, d.norgespris_period, m.intercept + m.slope * l.hdd AS expected
        FROM v_long l
        JOIN dim_date d ON d.date_key = l.date_key
        LEFT JOIN v_model m ON m.level = l.level AND m.area_code = l.area_code AND m.measure = l.measure
    """), "v_daily")


def build_agg_area_daily():
    """Norway, price areas and counties: actual vs expected per day and measure."""
    return spark.sql("""
        SELECT level, area_code, area_name, measure, date_key, date, norgespris_period,
               ROUND(value, 4) AS value, ROUND(expected, 4) AS expected,
               ROUND(hdd, 2) AS hdd, ROUND(mean_temp_c, 2) AS mean_temp_c,
               ROUND(norgespris_share_household, 4) AS norgespris_share_household
        FROM v_daily
        WHERE level <> 'municipality'
    """)


def build_agg_norgespris_effect():
    """One row per area and measure: the temperature-adjusted change after Norgespris (the answer to the question)."""
    keep(spark.sql(f"""
        WITH after AS (
            SELECT level, area_code, measure, COUNT(*) AS days_after, AVG(value) AS avg_after,
                   AVG(expected) AS avg_expected_after, SUM(value) AS sum_after, SUM(expected) AS sum_expected_after,
                   AVG(mean_temp_c) AS mean_temp_after
            FROM v_daily WHERE norgespris_period = 'after'
            GROUP BY level, area_code, measure),
        names AS (SELECT DISTINCT level, area_code, area_name FROM v_daily),
        latest AS (                                  -- Norgespris share of household meters on the newest day
            SELECT level, area_code, MAX_BY(norgespris_share_household, date_key) AS norgespris_share_latest
            FROM v_daily WHERE norgespris_share_household IS NOT NULL GROUP BY level, area_code)
        SELECT n.level, n.area_code, n.area_name, m.measure,
               m.days_before, COALESCE(a.days_after, 0) AS days_after,
               m.intercept, m.slope, m.r2,
               m.avg_before, a.avg_after, a.avg_expected_after,
               100 * (try_divide(a.sum_after, a.sum_expected_after) - 1) AS adjusted_change_pct,
               100 * (try_divide(a.avg_after, m.avg_before) - 1)        AS raw_change_pct,
               m.mean_temp_before, a.mean_temp_after, l.norgespris_share_latest,
               COALESCE(m.days_before >= {MIN_DAYS_BEFORE} AND a.days_after >= {MIN_DAYS_AFTER}
                        AND m.r2 >= {MIN_R2}, false)                    AS reliable
        FROM v_model m
        JOIN names n ON n.level = m.level AND n.area_code = m.area_code
        LEFT JOIN after a ON a.level = m.level AND a.area_code = m.area_code AND a.measure = m.measure
        LEFT JOIN latest l ON l.level = m.level AND l.area_code = m.area_code
    """), "v_effect")
    # second step: add the business control group (self-join on the stored view) and the ranking
    return spark.sql("""
        SELECT e.level, e.area_code, e.area_name, e.measure, e.days_before, e.days_after,
               ROUND(e.intercept, 4) AS intercept, ROUND(e.slope, 4) AS slope_per_hdd, ROUND(e.r2, 3) AS r2,
               ROUND(e.avg_before, 4) AS avg_before, ROUND(e.avg_after, 4) AS avg_after,
               ROUND(e.avg_expected_after, 4) AS avg_expected_after,
               ROUND(e.adjusted_change_pct, 2) AS adjusted_change_pct,
               ROUND(e.raw_change_pct, 2) AS raw_change_pct,
               ROUND(CASE WHEN e.measure <> 'kwh_per_business_meter'
                          THEN e.adjusted_change_pct - b.adjusted_change_pct END, 2) AS adjusted_change_vs_business_pp,
               ROUND(e.mean_temp_before, 2) AS mean_temp_before, ROUND(e.mean_temp_after, 2) AS mean_temp_after,
               ROUND(e.norgespris_share_latest, 4) AS norgespris_share_latest,
               e.reliable,
               CASE WHEN e.reliable THEN ROW_NUMBER() OVER (PARTITION BY e.level, e.measure, e.reliable
                                                           ORDER BY ABS(e.adjusted_change_pct) DESC) END AS rank_abs_change
        FROM v_effect e
        LEFT JOIN v_effect b ON b.level = e.level AND b.area_code = e.area_code AND b.measure = 'kwh_per_business_meter'
    """)


def build_agg_monthly_trend():
    """Area x measure x month: averages, adjusted change of the month, year-over-year change."""
    return spark.sql("""
        WITH m AS (
            SELECT level, area_code, area_name, measure, date_format(date, 'yyyy-MM') AS year_month,
                   trunc(date, 'MM') AS month_start, COUNT(*) AS days,
                   AVG(value) AS avg_value, AVG(expected) AS avg_expected, AVG(hdd) AS avg_hdd, AVG(mean_temp_c) AS mean_temp_c,
                   100 * (try_divide(SUM(value), SUM(expected)) - 1) AS adjusted_change_pct
            FROM v_daily WHERE level <> 'municipality'
            GROUP BY level, area_code, area_name, measure, date_format(date, 'yyyy-MM'), trunc(date, 'MM'))
        SELECT m.level, m.area_code, m.area_name, m.measure, m.year_month, m.month_start, m.days,
               CASE WHEN m.month_start >= DATE '{start}' THEN 'after' ELSE 'before' END AS norgespris_period,
               ROUND(m.avg_value, 4) AS avg_value, ROUND(m.avg_expected, 4) AS avg_expected,
               ROUND(m.avg_hdd, 2) AS avg_hdd, ROUND(m.mean_temp_c, 2) AS mean_temp_c,
               ROUND(m.adjusted_change_pct, 2) AS adjusted_change_pct,
               ROUND(100 * (try_divide(m.avg_value, p.avg_value) - 1), 2) AS yoy_change_pct,
               ROUND(m.avg_hdd - p.avg_hdd, 2) AS yoy_hdd_diff
        FROM m
        LEFT JOIN m p ON p.level = m.level AND p.area_code = m.area_code AND p.measure = m.measure
                     AND p.month_start = add_months(m.month_start, -12)
    """.replace("{start}", str(NORGESPRIS_START)))


def build_agg_norgespris_uptake_daily():
    """Day x price area x consumption group (after the start): share of meters and of kWh on Norgespris."""
    return spark.sql(f"""
        WITH meters AS (                             -- Norgespris meters per municipality, summed per price area
            SELECT date_key, price_area_key, consumption_group_key,
                   SUM(norgespris_metering_points) AS norgespris_metering_points,
                   SUM(counted_metering_points) AS counted_metering_points
            FROM fact_private_split_daily
            WHERE date_key >= {NORGESPRIS_START:%Y%m%d} AND counted_metering_points IS NOT NULL
            GROUP BY date_key, price_area_key, consumption_group_key),
        kwh AS (
            SELECT date_key, price_area_key, consumption_group_key,
                   SUM(kwh_norgespris) AS kwh_norgespris, SUM(kwh_not_norgespris) AS kwh_not_norgespris
            FROM fact_norgespris_price_area_daily
            WHERE date_key >= {NORGESPRIS_START:%Y%m%d}
            GROUP BY date_key, price_area_key, consumption_group_key)
        SELECT date_key, price_area_key, consumption_group_key,
               norgespris_metering_points, counted_metering_points,
               ROUND(try_divide(CAST(norgespris_metering_points AS DOUBLE), counted_metering_points), 4) AS metering_point_share,
               kwh_norgespris, kwh_not_norgespris,
               ROUND(try_divide(CAST(kwh_norgespris AS DOUBLE), kwh_norgespris + kwh_not_norgespris), 4) AS kwh_share
        FROM meters FULL OUTER JOIN kwh USING (date_key, price_area_key, consumption_group_key)
    """)


TABLES = ["dim_date", "dim_municipality", "dim_customer_group", "dim_consumption_group",      # order = build + write order
          "dim_price_area",
          "fact_consumption_daily", "fact_private_split_daily", "fact_weather_daily", "fact_population_yearly",
          "fact_norgespris_price_area_daily",
          "agg_municipality_daily", "agg_area_daily", "agg_norgespris_effect", "agg_monthly_trend",
          "agg_norgespris_uptake_daily"]
KEEP = {"dim_date", "dim_municipality", "dim_customer_group", "dim_consumption_group",         # read again by later steps
        "dim_price_area",
        "fact_consumption_daily", "agg_municipality_daily"}


def date_bounds(silver):
    """(first day, last day) over all Silver dates, incl. 1 January of every population year."""
    parts = [silver[t].select(F.min("date").alias("lo"), F.max("date").alias("hi"))
             for t in ("consumption_daily", "temperature_municipality_daily",
                       "norgespris_count_daily", "norgespris_price_area_daily")]
    parts.append(silver["population_yearly"].select(F.expr("make_date(min(year), 1, 1)").alias("lo"),
                                                    F.expr("make_date(max(year), 1, 1)").alias("hi")))
    row = reduce(lambda a, b: a.unionByName(b), parts).agg(F.min("lo").alias("lo"), F.max("hi").alias("hi")).first()
    return row["lo"], row["hi"]


def build_gold_tables(silver):
    """silver = {silver table name: DataFrame} -> {gold table name: DataFrame}, built lazily in dependency order."""
    for name, df in silver.items():
        df.createOrReplaceTempView(f"silver_{name}")             # e.g. silver_consumption_daily
    first_day, last_day = date_bounds(silver)
    if first_day is None:
        raise ValueError("Silver has no dates - is silver.consumption_daily empty?")
    consumption_groups = (silver["norgespris_count_daily"].select("consumption_group")       # Norgespris groups
                          .unionByName(silver["norgespris_price_area_daily"].select("consumption_group")))
    builders = {
        "dim_date": lambda: build_dim_date(first_day, last_day),
        "dim_municipality": lambda: build_dim_municipality(silver["municipality"]),
        "dim_customer_group": lambda: build_dim_customer_group(silver["consumption_daily"]),
        "dim_consumption_group": lambda: build_dim_consumption_group(consumption_groups),
        "dim_price_area": lambda: build_dim_price_area(silver["norgespris_price_area_daily"]),
        "fact_consumption_daily": build_fact_consumption_daily,
        "fact_private_split_daily": build_fact_private_split_daily,
        "fact_weather_daily": build_fact_weather_daily,
        "fact_population_yearly": build_fact_population_yearly,
        "fact_norgespris_price_area_daily": build_fact_norgespris_price_area_daily,
        "agg_municipality_daily": build_agg_municipality_daily,
        "agg_area_daily": build_agg_area_daily,
        "agg_norgespris_effect": build_agg_norgespris_effect,
        "agg_monthly_trend": build_agg_monthly_trend,
        "agg_norgespris_uptake_daily": build_agg_norgespris_uptake_daily,
    }
    gold = {}
    for table in TABLES:
        if table == "agg_area_daily":                           # the area views need agg_municipality_daily
            build_area_views()
        df = builders[table]()
        if table in KEEP:
            df = keep(df, table)                                # computed once now, reused by the next steps
        else:
            df.createOrReplaceTempView(table)                   # later SQL can use the table by its name
        gold[table] = df
    return gold


PRIMARY_KEYS = {                                                # the grain of every Gold table
    "dim_date": ["date_key"],
    "dim_municipality": ["municipality_key"],
    "dim_customer_group": ["customer_group_key"],
    "dim_consumption_group": ["consumption_group_key"],
    "dim_price_area": ["price_area_key"],
    "fact_consumption_daily": ["date_key", "municipality_key", "customer_group_key"],
    "fact_private_split_daily": ["date_key", "municipality_key", "consumption_group_key"],
    "fact_weather_daily": ["date_key", "municipality_key"],
    "fact_population_yearly": ["municipality_key", "year"],
    "fact_norgespris_price_area_daily": ["date_key", "price_area_key", "consumption_group_key", "consumption_band"],
    "agg_municipality_daily": ["municipality_key", "date_key"],
    "agg_area_daily": ["level", "area_code", "measure", "date_key"],
    "agg_norgespris_effect": ["level", "area_code", "measure"],
    "agg_monthly_trend": ["level", "area_code", "measure", "year_month"],
    "agg_norgespris_uptake_daily": ["date_key", "price_area_key", "consumption_group_key"],
}
DIMENSION_OF = {"date_key": "dim_date", "municipality_key": "dim_municipality", "customer_group_key": "dim_customer_group",
                "consumption_group_key": "dim_consumption_group", "price_area_key": "dim_price_area"}
REFERENCES = {                                                  # table -> its key columns that point to a dimension
    "fact_consumption_daily": ["date_key", "municipality_key", "customer_group_key", "price_area_key"],
    "fact_private_split_daily": ["date_key", "municipality_key", "consumption_group_key", "price_area_key"],
    "fact_weather_daily": ["date_key", "municipality_key", "price_area_key"],
    "fact_population_yearly": ["date_key", "municipality_key", "price_area_key"],
    "fact_norgespris_price_area_daily": ["date_key", "price_area_key", "consumption_group_key"],
    "agg_municipality_daily": ["date_key", "municipality_key"],
    "agg_area_daily": ["date_key"],
    "agg_norgespris_uptake_daily": ["date_key", "price_area_key", "consumption_group_key"],
}
FOREIGN_KEYS = [(table, column, DIMENSION_OF[column]) for table, columns in REFERENCES.items() for column in columns]


def gold_checks(s=STAGE):
    """[(check_id, severity, description, sql for the Gold number, sql for the expected number, rule), ...]"""
    checks = [
        ("G-01", "error", "kWh: fact_consumption_daily = silver.consumption_daily",
         f"SELECT SUM(kwh) FROM {s}.fact_consumption_daily", "SELECT SUM(kwh) FROM silver.consumption_daily", "equal"),
        ("G-02", "error", "rows with a known municipality: fact = silver",
         f"SELECT COUNT(*) FROM {s}.fact_consumption_daily WHERE municipality_key <> -1",
         "SELECT COUNT(*) FROM silver.consumption_daily WHERE known_municipality", "equal"),
        ("G-03", "error", "population: fact_population_yearly = silver.population_yearly",
         f"SELECT SUM(population) FROM {s}.fact_population_yearly", "SELECT SUM(population) FROM silver.population_yearly",
         "equal"),
        ("G-04", "error", "municipality-days with temperature: fact_weather_daily = silver",
         f"SELECT COUNT(*) FROM {s}.fact_weather_daily",
         "SELECT COUNT(*) FROM silver.temperature_municipality_daily WHERE mean_temp_c IS NOT NULL", "equal"),
        ("G-05", "error", "kWh: agg_municipality_daily = known municipalities in the fact",
         f"SELECT SUM(kwh_all) FROM {s}.agg_municipality_daily",
         f"SELECT SUM(kwh) FROM {s}.fact_consumption_daily WHERE municipality_key <> -1", "equal"),
        ("G-11", "error", "kWh: fact_private_split_daily = silver.private_split_daily (household + cabin)",
         f"SELECT SUM(kwh) FROM {s}.fact_private_split_daily",
         "SELECT SUM(kwh_household + kwh_cabin) FROM silver.private_split_daily", "equal"),
    ]
    for table, columns in PRIMARY_KEYS.items():
        cols = ", ".join(columns)
        checks.append((f"G-06 {table}", "error", f"grain is unique ({cols})",
                       f"SELECT COUNT(*) FROM (SELECT 1 FROM {s}.{table} GROUP BY {cols} HAVING COUNT(*) > 1) x",
                       "SELECT 0", "equal"))
    for table, column, dim in FOREIGN_KEYS:
        checks.append((f"G-07 {table}.{column}", "error", f"every {column} exists in {dim}",
                       f"SELECT COUNT(*) FROM {s}.{table} f WHERE NOT EXISTS "
                       f"(SELECT 1 FROM {s}.{dim} d WHERE d.{column} = f.{column})", "SELECT 0", "equal"))
    checks += [
        ("G-08", "error", "upstream: newest Silver run has 0 duplicate keys",
         "SELECT COALESCE(SUM(value), 0) FROM silver.quality_metrics WHERE metric = 'duplicate_keys' "
         "AND run_id = (SELECT MAX(run_id) FROM silver.quality_metrics)", "SELECT 0", "equal"),
        ("G-09", "warning", "municipality-days without temperature",
         f"SELECT COUNT(*) FROM {s}.agg_municipality_daily WHERE hdd IS NULL", "SELECT 0", "equal"),
        ("G-12", "warning", "kWh: household + cabin = measured private kWh (private kWh without a distribution key)",
         f"SELECT SUM(kwh) FROM {s}.fact_private_split_daily",
         "SELECT SUM(kwh) FROM silver.consumption_daily WHERE customer_group = 'private' AND known_municipality", "equal"),
        ("G-10", "warning", "days since the newest consumption day (fresh data)",
         f"SELECT current_date - MAX(d.date) FROM {s}.fact_consumption_daily f "
         f"JOIN {s}.dim_date d ON d.date_key = f.date_key", "SELECT 45", "at_most"),
    ]
    return checks


def run_checks(run_id, s=STAGE):
    """Run every check in PostgreSQL; returns a list of result dicts."""
    results = []
    for check_id, severity, description, sql_gold, sql_expected, rule in gold_checks(s):
        try:
            row = pg_query(f"SELECT CAST(({sql_gold}) AS DOUBLE PRECISION) AS g, "
                           f"CAST(({sql_expected}) AS DOUBLE PRECISION) AS e").first()
            g, e = row["g"], row["e"]
            if rule == "equal":
                passed = g is not None and e is not None and abs(g - e) <= 0.01
            else:                                               # at_most
                passed = g is not None and g <= e
        except Exception as error:                              # e.g. a table is missing -> the check fails
            g, e, passed = None, None, False
            description = f"{description} (query failed: {str(error).splitlines()[0][:150]})"
        results.append({"run_id": run_id, "check_id": check_id, "severity": severity, "description": description,
                        "gold_value": g, "expected_value": e, "passed": passed})
        if not passed:
            logger.warning(f"  check {check_id} ({severity}) FAILED: {description}: gold={g}, expected={e}")
    return results


def add_constraints(s=STAGE):
    """Primary keys and foreign keys on the staged tables (one transaction)."""
    statements = [f"ALTER TABLE {s}.{t} ADD PRIMARY KEY ({', '.join(cols)})" for t, cols in PRIMARY_KEYS.items()]
    statements += [f"ALTER TABLE {s}.{t} ADD FOREIGN KEY ({c}) REFERENCES {s}.{dim} ({c})" for t, c, dim in FOREIGN_KEYS]
    pg_execute(*statements)


def publish(tables):
    """Swap gold_stage -> gold in one transaction; the tables that were live move to gold_previous."""
    live = {r["table_name"] for r in pg_query(
        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{LIVE}'").collect()}
    pg_execute(f"CREATE SCHEMA IF NOT EXISTS {LIVE}",
               f"DROP SCHEMA IF EXISTS {PREVIOUS} CASCADE",      # the version before the previous one is dropped
               f"CREATE SCHEMA {PREVIOUS}",
               *[f"ALTER TABLE {LIVE}.{t} SET SCHEMA {PREVIOUS}" for t in tables if t in live],
               *[f"ALTER TABLE {STAGE}.{t} SET SCHEMA {LIVE}" for t in tables])


def log_run(run_id, status, silver_run_id, first_day, last_day, checks, rows, elapsed, message=""):
    """Append one row to gold.publish_log and the check results to gold.check_results; write _gold_summary.md."""
    failed = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "error"]
    warnings = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "warning"]
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log = spark.createDataFrame(
        [(run_id, now, ENV, status, silver_run_id, str(first_day or ""), str(last_day or ""),
          len(checks), ", ".join(failed), ", ".join(warnings), message, float(elapsed))],
        "run_id string, logged_at string, env string, status string, silver_run_id string, first_day string, "
        "last_day string, n_checks int, failed_checks string, warning_checks string, message string, elapsed_s double")
    pg_execute(f"CREATE SCHEMA IF NOT EXISTS {LIVE}")
    log.write.jdbc(PG_URL, f"{LIVE}.publish_log", mode="append", properties=PG_PROPERTIES)
    if checks:
        (spark.createDataFrame([(c["run_id"], c["check_id"], c["severity"], c["description"], c["gold_value"],
                                 c["expected_value"], c["passed"]) for c in checks],
                               "run_id string, check_id string, severity string, description string, "
                               "gold_value double, expected_value double, passed boolean")
              .write.jdbc(PG_URL, f"{LIVE}.check_results", mode="append", properties=PG_PROPERTIES))
    lines = [f"## Gold run {run_id} ({ENV}): **{status.upper()}**", "",
             f"Data {first_day} .. {last_day} | Silver run {silver_run_id} | {elapsed:.0f}s | {message}", "",
             "| Table | Rows |", "|---|---|"] + [f"| {t} | {n:,} |" for t, n in rows.items()]
    lines += ["", "| Check | Severity | Gold | Expected | OK |", "|---|---|---|---|---|"]
    lines += [f"| {c['check_id']} | {c['severity']} | {c['gold_value']} | {c['expected_value']} | "
              f"{'✅' if c['passed'] else '❌'} |" for c in checks]
    os.makedirs(LANDING_DIR, exist_ok=True)
    with open(os.path.join(LANDING_DIR, "_gold_summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def run_gold():
    """Build, check and publish the Gold layer; returns a summary dict (same contract as the other stages)."""
    global spark
    start_time = time.time()
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):
        logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")
        return {"status": "failed", "elapsed_time": 0.0}
    spark = get_spark()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    available = silver_tables()
    required = ["municipality", "population_yearly", "consumption_daily", "temperature_municipality_daily",
                "private_split_daily"]
    missing = [t for t in required if t not in available]
    silver_run_id = None
    if "quality_metrics" in available:
        silver_run_id = pg_query("SELECT MAX(run_id) AS r FROM silver.quality_metrics").first()["r"]
    if missing:                                                 # upstream incomplete -> keep the published version
        message = f"missing Silver tables {missing} - run the silver stage first"
        logger.error(message)
        log_run(run_id, "blocked", silver_run_id, None, None, [], {}, time.time() - start_time, message)
        return {"status": "failed", "elapsed_time": time.time() - start_time, "run_id": run_id}
    logger.info(f"Gold run {run_id} (env={ENV}, Silver run {silver_run_id}) -> {PG_URL}")
    pg_execute(f"DROP SCHEMA IF EXISTS {STAGE} CASCADE", f"CREATE SCHEMA {STAGE}")   # a clean build area every run

    silver = {
        "municipality": read_silver("municipality"),
        "population_yearly": read_silver("population_yearly"),
        "consumption_daily": read_silver("consumption_daily", by_month=True).persist(StorageLevel.MEMORY_AND_DISK),
        "temperature_municipality_daily": read_silver("temperature_municipality_daily", by_month=True),
        "norgespris_count_daily": read_optional("norgespris_count_daily", available, by_month=True),
        "norgespris_price_area_daily": read_optional("norgespris_price_area_daily", available),
        "private_split_daily": read_silver("private_split_daily", by_month=True),
    }
    first_day, last_day = date_bounds(silver)
    gold = build_gold_tables(silver)
    rows, timings = {}, {}
    for table in TABLES:                                        # write in dependency order
        t0 = time.time()
        rows[table] = write_stage(gold[table], table)
        timings[table] = round(time.time() - t0, 1)

    checks = run_checks(run_id)
    failed = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "error"]
    status, message = "published", ""
    if failed:
        status, message = "blocked", f"{len(failed)} error check(s) failed - gold keeps the previous version"
    else:
        try:
            add_constraints()
            publish(TABLES)
        except Exception as error:                              # e.g. a constraint cannot be created
            status, message = "blocked", f"publishing failed: {str(error).splitlines()[0][:300]}"
    elapsed = time.time() - start_time
    log_run(run_id, status, silver_run_id, first_day, last_day, checks, rows, elapsed, message)
    spark.catalog.clearCache()                                  # free the memory of every persisted table and view
    (logger.info if status == "published" else logger.error)(
        f"Gold {status} in {elapsed:.1f}s ({len(checks)} checks, failed: {failed or 'none'}) {message}")
    return {"status": "success" if status == "published" else "failed", "elapsed_time": elapsed, "run_id": run_id,
            "published": status == "published", "rows": rows, "timings_s": timings, "failed_checks": failed}


if __name__ == "__main__":
    result = run_gold()                                        # build, check and publish Gold
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
