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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # log format: time - logger - level - message
logger = logging.getLogger("gold")                              # own logger name in the run log

spark = None                                                    # the SparkSession, set in run_gold() / the notebook
JDBC_READ = dict(PG_PROPERTIES, fetchsize="10000")              # stream 10 000 rows at a time (default: ALL rows into memory)

NORGESPRIS_START = date(2025, 10, 1)                            # first day with Norgespris (regjeringen.no)
HDD_BASE_C = 17.0                                               # heating degree days: base temperature 17 °C (Norwegian standard)
MIN_DAYS_BEFORE = 180                                           # a result is "reliable" with >= 180 days before the start ...
MIN_DAYS_AFTER = 28                                             # ... >= 28 days after the start ...
MIN_R2 = 0.3                                                    # ... and when temperature explains >= 30 % of the daily variation
STAGE, LIVE, PREVIOUS = "gold_stage", "gold", "gold_previous"   # build -> check -> publish; the version before is kept


PRICE_AREAS = {"NO1": "Øst-Norge", "NO2": "Sør-Norge", "NO3": "Midt-Norge", "NO4": "Nord-Norge", "NO5": "Vest-Norge"}  # price area code -> usual name
CUSTOMER_GROUPS = {                                             # silver.consumption_daily.customer_group
    "private": (1, "Private (homes + cabins)", True),           # -> (key, description, Norgespris eligible)
    "business": (2, "Business (næring)", False),                # business: not eligible
    "industry": (3, "Industry (industri)", False),              # industry: not eligible
}                                                               # end of the customer groups
CONSUMPTION_GROUPS = {                                          # consumption_group in the two Norgespris tables
    "household": (1, "Household (bolig)", 5000),                # -> (key, description, cap kWh/month)
    "cabin": (2, "Cabin / holiday home (fritidsbolig)", 1000),  # cabins: cap 1 000 kWh per month
}                                                               # end of the consumption groups
MEASURES = {                                                    # measure name -> what it means (shown in the dashboard)
    "kwh_per_private_meter": "kWh per private metering point per day (measured)",  # measured, no key involved
    "kwh_per_household": "kWh per household metering point per day (allocated)",  # allocated with the key
    "kwh_household_per_capita": "household kWh per inhabitant per day (allocated)",  # allocated with the key
    "kwh_per_cabin": "kWh per cabin metering point per day (allocated)",  # allocated with the key
    "kwh_all_per_capita": "all kWh (private + business + industry) per inhabitant per day (measured)",  # measured
    "kwh_per_business_meter": "kWh per business metering point per day (control group, no Norgespris)",  # measured, control group
}                                                               # end of the measures


def pg_query(sql):                                              # helper: SELECT inside PostgreSQL
    """Run a SELECT inside PostgreSQL and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=JDBC_READ)  # the query is wrapped as a subquery


def pg_execute(*statements):                                    # helper: statements without a result
    """Run statements that return nothing (CREATE/ALTER/DROP) in ONE transaction, via Spark's JDBC driver."""
    jvm = spark.sparkContext._jvm                               # the Java side of Spark, where the PostgreSQL driver is loaded
    props = jvm.java.util.Properties()                          # user + password for the connection
    props.setProperty("user", PG_PROPERTIES["user"])            # user name
    props.setProperty("password", PG_PROPERTIES["password"])    # password
    conn = jvm.org.postgresql.Driver().connect(PG_URL, props)   # the same driver Spark uses to read and write
    try:                                                        # run all statements ...
        conn.setAutoCommit(False)                               # start a transaction: all or nothing
        statement = conn.createStatement()                      # object that sends SQL
        for sql in statements:                                  # one statement after the other
            statement.execute(sql)                              # send it to PostgreSQL
        conn.commit()                                           # make all changes visible at the same moment
    except Exception:                                           # any statement failed
        conn.rollback()                                         # undo everything -> nothing half-done
        raise                                                   # pass the error on
    finally:                                                    # always:
        conn.close()                                            # close the connection


def silver_tables():                                            # which tables Silver has
    """Names of the tables in the silver schema."""
    rows = pg_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").collect()  # ask PostgreSQL's catalog
    return {r["table_name"] for r in rows}                      # set of table names


def read_silver(table, by_month=False):                         # read one Silver table
    """Read silver.<table>; with by_month=True one Spark partition per month of the 'date' column."""
    if not by_month:                                            # small table -> one partition is fine
        return spark.read.jdbc(PG_URL, f"silver.{table}", properties=JDBC_READ)  # one JDBC connection
    months = [r["m"] for r in pg_query(f"SELECT DISTINCT to_char(date, 'YYYY-MM-01') AS m "  # first day of every month in the data ...
                                       f"FROM silver.{table} WHERE date IS NOT NULL").collect()]  # ... from this table
    predicates = [f"date >= DATE '{m}' AND date < DATE '{m}' + INTERVAL '1 month'" for m in sorted(months)]  # one WHERE per month = one partition
    predicates.append("date IS NULL")                           # (Silver has none, but no row may ever be lost)
    return spark.read.jdbc(PG_URL, f"silver.{table}", predicates=predicates, properties=JDBC_READ)  # parallel read


def empty_silver(table):                                        # empty table with the Silver columns
    """An empty DataFrame with the columns of silver.<table>."""
    return spark.createDataFrame([], ", ".join(f"{name} {dtype}" for name, dtype in SILVER_SCHEMAS[table]))  # no rows, Silver schema


def read_optional(table, available, by_month=False):            # read an optional Silver table
    """silver.<table> if it exists, otherwise an empty table with the same columns."""
    if table in available:                                      # it exists ...
        return read_silver(table, by_month)                     # ... read it
    logger.info(f"  silver.{table} does not exist - Gold continues without it (optional)")  # say it is missing (not an error)
    return empty_silver(table)                                  # empty instead


def write_stage(df, table):                                     # write one table to gold_stage
    """Replace gold_stage.<table> with df; returns the number of rows now in PostgreSQL."""
    (df.write                                                   # write ...
       .option("batchsize", 10000)                              # rows per INSERT batch
       .jdbc(PG_URL, f"{STAGE}.{table}", mode="overwrite", properties=PG_PROPERTIES))  # ... over JDBC, replacing the table
    rows = pg_query(f"SELECT COUNT(*) AS n FROM {STAGE}.{table}").first()["n"]  # count inside the database
    logger.info(f"  {STAGE}.{table}: {rows:,} rows")            # log the row count
    return rows                                                 # row count


def date_key(column):                                           # SQL for a yyyymmdd key
    """SQL for the integer date key yyyymmdd of a date column (2025-10-01 -> 20251001)."""
    return f"CAST(date_format({column}, 'yyyyMMdd') AS INT)"    # e.g. 20251001


def build_dim_date(first_day, last_day):                        # the calendar dimension
    """One row per day from first_day to last_day, with calendar and Norgespris attributes."""
    return spark.sql(f"""                                       -- one row per day, from Spark SQL
        SELECT {date_key('d')}                                             AS date_key,  -- key yyyymmdd
               d                                                           AS date,  -- the date itself
               year(d)                                                     AS year,  -- calendar year
               quarter(d)                                                  AS quarter,  -- quarter 1-4
               month(d)                                                    AS month,  -- month 1-12
               date_format(d, 'MMMM')                                      AS month_name,  -- month name, e.g. October
               date_format(d, 'yyyy-MM')                                   AS year_month,  -- e.g. 2025-10
               weekofyear(d)                                               AS iso_week,  -- ISO week number
               weekday(d) + 1                                              AS day_of_week,     -- 1 = Monday
               date_format(d, 'EEEE')                                      AS day_name,  -- e.g. Monday
               weekday(d) >= 5                                             AS is_weekend,  -- Saturday or Sunday
               CASE WHEN month(d) IN (12, 1, 2) THEN 'winter' WHEN month(d) IN (3, 4, 5) THEN 'spring'  -- season of the month ...
                    WHEN month(d) IN (6, 7, 8) THEN 'summer' ELSE 'autumn' END   AS season,  -- ... winter, spring, summer, autumn
               month(d) IN (10, 11, 12, 1, 2, 3, 4)                        AS is_heating_season,  -- October to April
               CASE WHEN month(d) >= 7                                      -- July-June, e.g. '2025/26'
                    THEN concat(CAST(year(d) AS STRING), '/', substr(CAST(year(d) + 1 AS STRING), 3, 2))  -- July-December: this/next year
                    ELSE concat(CAST(year(d) - 1 AS STRING), '/', substr(CAST(year(d) AS STRING), 3, 2)) END AS energy_year,  -- January-June: last/this year
               CASE WHEN d >= DATE '{NORGESPRIS_START}' THEN 'after' ELSE 'before' END AS norgespris_period,  -- before or after the start
               datediff(d, DATE '{NORGESPRIS_START}')                      AS days_since_norgespris  -- 0 on the first Norgespris day
        FROM (SELECT explode(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL 1 DAY)) AS d)  -- one row per day in the range
    """)                                                        # end of the SQL


def price_area_key(column):                                     # price area key
    """'NO3' -> 3, anything else -> -1 (unknown)."""
    return F.when(column.rlike("^NO[0-9]$"), F.substring(column, 3, 1).cast("int")).otherwise(F.lit(-1))  # NO3 -> 3, else -1


def build_dim_municipality(municipality):                       # the municipality dimension
    """silver.municipality (with county name + main price area from Silver), plus one 'unknown' row (key -1)."""
    known = (municipality                                       # from Silver's municipality table:
             .select(F.expr("try_cast(municipality_no AS INT)").alias("municipality_key"),  # '0301' -> 301 (stable)
                     "municipality_no", "municipality_name", "county_no",  # natural keys and names
                     F.coalesce("county_name", F.lit("Unknown county")).alias("county_name"),  # county name, never NULL
                     F.coalesce("price_area", F.lit("unknown")).alias("price_area"),  # price area, never NULL
                     F.lit(True).alias("is_known")))            # a real municipality
    unknown = spark.createDataFrame([(-1, "unknown", "Unknown municipality", "00", "Unknown county", "unknown", False)],  # one extra row for unknown codes ...
                                    known.schema)              # for Elhub code 0 and codes SSB does not know
    dim = known.unionByName(unknown)                            # known + unknown
    return dim.withColumn("price_area_key", price_area_key(F.col("price_area")))  # + the price area key


def build_dim_price_area(norgespris):                           # the price area dimension
    """NO1-NO5 + unknown, plus any other code found in the Norgespris price-area data."""
    rows = list(PRICE_AREAS.items()) + [("unknown", "Unknown price area")]  # the five areas + unknown
    static = spark.createDataFrame(rows, "price_area string, price_area_name string")  # as a small DataFrame
    extra = (norgespris.select("price_area").where("price_area IS NOT NULL").distinct()  # codes in the data ...
             .join(static, "price_area", "left_anti")         # codes we did not expect
             .withColumn("price_area_name", F.col("price_area")))  # ... named by their code
    dim = static.unionByName(extra)                             # known + unexpected
    return dim.select(price_area_key(F.col("price_area")).alias("price_area_key"), "price_area", "price_area_name")  # key, code, name


def add_new_groups(known, groups, column):                      # add new groups with keys 100, 101, ...
    """known dimension rows + one row (key 100, 101, ...) for every value of `column` in the data that is not known."""
    known_names = {r[column] for r in known.select(column).collect()}            # 2-3 names
    found = groups.select(column).where(f"{column} IS NOT NULL").distinct().collect()  # the few names in the data
    new_names = sorted(r[column] for r in found if r[column] not in known_names)  # values we did not expect
    if not new_names:                                           # nothing new
        return known                                            # the known rows only
    new = spark.createDataFrame([(100 + i, name, name) for i, name in enumerate(new_names)],   # numbered in Python:
                                f"{column}_key int, {column} string, description string")     # a handful of rows
    return known.unionByName(new, allowMissingColumns=True)     # the other columns stay NULL for a new group


def build_dim_customer_group(groups):                           # the customer group dimension
    """Customer groups of silver.consumption_daily: private/business/industry with fixed keys, new ones from 100."""
    known = spark.createDataFrame(                              # known groups as a DataFrame ...
        [(key, name, text, eligible) for name, (key, text, eligible) in CUSTOMER_GROUPS.items()],  # ... from CUSTOMER_GROUPS ...
        "customer_group_key int, customer_group string, description string, norgespris_eligible boolean")  # ... with these columns
    dim = add_new_groups(known, groups, "customer_group")       # + new groups from the data
    return dim.withColumn("norgespris_eligible", F.coalesce("norgespris_eligible", F.lit(False)))  # new -> not eligible


def build_dim_consumption_group(groups):                        # the consumption group dimension
    """Norgespris consumption groups of the two Norgespris tables: household/cabin with fixed keys, new ones from 100."""
    known = spark.createDataFrame(                              # known groups as a DataFrame ...
        [(key, name, text, cap) for name, (key, text, cap) in CONSUMPTION_GROUPS.items()],  # ... from CONSUMPTION_GROUPS ...
        "consumption_group_key int, consumption_group string, description string, norgespris_cap_kwh_month int")  # ... with these columns
    return add_new_groups(known, groups, "consumption_group")   # + new groups from the data


def build_fact_consumption_daily():                             # fact: consumption per day and customer group
    """municipality x day x customer group: kWh and meter counts with dimension keys (unknown municipality -> -1)."""
    return spark.sql(f"""                                       -- Spark SQL
        SELECT /*+ BROADCAST(m, g) */                           -- small dimensions are broadcast
               {date_key('c.date')}                         AS date_key,  -- date key
               COALESCE(m.municipality_key, -1)             AS municipality_key,  -- municipality key, -1 = unknown
               g.customer_group_key,                            -- customer group key
               COALESCE(m.price_area_key, -1)               AS price_area_key,  -- price area key, -1 = unknown
               CAST(SUM(c.kwh) AS DECIMAL(18,3))            AS kwh,  -- kWh
               CAST(SUM(c.metering_points) AS INT)          AS metering_points,  -- metering points
               BOOL_AND(c.complete_day)                     AS complete_day  -- true if every row had all hours
        FROM silver_consumption_daily c                         -- from Silver
        JOIN dim_customer_group g ON g.customer_group = c.customer_group  -- customer group key
        LEFT JOIN dim_municipality m ON m.municipality_no = c.municipality_no AND c.known_municipality  -- municipality key (known only)
        GROUP BY 1, 2, 3, 4                                     -- unknown codes are summed into key -1
    """)                                                        # end of the SQL


def build_fact_private_split_daily():                           # fact: private split into household + cabin
    """municipality x day x consumption group (household/cabin): private kWh and meters allocated by Silver's key."""
    return spark.sql(f"""                                       -- Spark SQL
        WITH long AS (                                 -- household and cabin columns -> one row per consumption group
            SELECT s.municipality_no, s.date, s.key_method, x.*  -- keys + the stacked values
            FROM silver_private_split_daily s                   -- from Silver's split table
            LATERAL VIEW stack(2,                               -- two rows per input row:
                'household', s.kwh_household, s.metering_points_household, s.norgespris_meters_household,  -- household values ...
                             s.counted_meters_household, s.household_kwh_share,  -- ... and its share
                'cabin',     s.kwh_cabin, s.metering_points_cabin, s.norgespris_meters_cabin,  -- cabin values ...
                             s.counted_meters_cabin, 1 - s.household_kwh_share)  -- ... and its share
                x AS consumption_group, kwh, metering_points, norgespris_metering_points, counted_metering_points,  -- names of the stacked columns ...
                     share_of_private_kwh                       -- ... last one
            WHERE s.household_kwh_share IS NOT NULL)        -- no key -> nothing to allocate
        SELECT /*+ BROADCAST(m, g) */                           -- small dimensions are broadcast
               {date_key('l.date')}                         AS date_key,  -- date key
               m.municipality_key,                              -- municipality key
               g.consumption_group_key,                         -- consumption group key
               m.price_area_key,                                -- price area key
               l.kwh, l.metering_points, l.norgespris_metering_points, l.counted_metering_points,  -- allocated kWh, meters, Norgespris meters
               ROUND(l.share_of_private_kwh, 4)             AS share_of_private_kwh,  -- share of the private kWh
               l.key_method                                     -- how the key was found
        FROM long l                                             -- the stacked rows
        JOIN dim_municipality m ON m.municipality_no = l.municipality_no  -- municipality key
        JOIN dim_consumption_group g ON g.consumption_group = l.consumption_group  -- consumption group key
    """)                                                        # end of the SQL


def build_fact_weather_daily():                                 # fact: temperature per municipality and day
    """municipality x day: temperature and heating degree days."""
    return spark.sql(f"""                                       -- Spark SQL
        SELECT /*+ BROADCAST(m) */                              -- small dimension is broadcast
               {date_key('t.date')}                         AS date_key,  -- date key
               m.municipality_key, m.price_area_key,            -- municipality and price area keys
               t.mean_temp_c,                                   -- mean temperature
               CAST(GREATEST({HDD_BASE_C} - t.mean_temp_c, 0) AS DECIMAL(5,2)) AS hdd,   -- heating degree days
               t.n_stations,                                    -- stations used
               t.method                                     AS temperature_method  -- own stations or county average
        FROM silver_temperature_municipality_daily t            -- from Silver
        JOIN dim_municipality m ON m.municipality_no = t.municipality_no  -- municipality key
        WHERE t.mean_temp_c IS NOT NULL                         -- only days with a temperature
    """)                                                        # end of the SQL


def build_fact_population_yearly():                             # fact: population on 1 January
    """municipality x year: population on 1 January."""
    return spark.sql("""                                        -- Spark SQL
        SELECT /*+ BROADCAST(m) */                              -- small dimension is broadcast
               p.year * 10000 + 101 AS date_key,           -- 1 January of that year
               m.municipality_key, m.price_area_key, p.year, p.population  -- keys and population
        FROM silver_population_yearly p                         -- from Silver
        JOIN dim_municipality m ON m.municipality_no = p.municipality_no  -- municipality key
    """)                                                        # end of the SQL


def build_fact_norgespris_price_area_daily():                   # fact: Norgespris per price area
    """day x price area x consumption group x consumption band: kWh and meters with / without Norgespris."""
    def limit(c):                                               # 3000.000 -> '3000', NULL -> '?'
        return f"COALESCE(CAST(CAST({c} AS BIGINT) AS STRING), '?')"  # SQL: limit as text, ? if missing
    return spark.sql(f"""                                       -- Spark SQL
        SELECT /*+ BROADCAST(a, g) */                           -- small dimensions are broadcast
               {date_key('n.date')}                         AS date_key,  -- date key
               COALESCE(a.price_area_key, -1)               AS price_area_key,  -- price area key
               g.consumption_group_key,                         -- consumption group key
               CASE WHEN n.eac_from_kwh IS NULL AND n.eac_to_kwh IS NULL THEN 'unknown'  -- band label: unknown ...
                    WHEN n.eac_to_kwh IS NULL OR n.eac_to_kwh >= 1e9 THEN concat({limit('n.eac_from_kwh')}, '+')  -- ... open upper limit ...
                    ELSE concat({limit('n.eac_from_kwh')}, '-', {limit('n.eac_to_kwh')}) END AS consumption_band,  -- ... or from-to
               n.eac_from_kwh, n.eac_to_kwh,                    -- band limits
               n.kwh_norgespris, n.kwh_not_norgespris, n.kwh_total,  -- kWh with, without, total
               n.metering_points_norgespris, n.metering_points_not_norgespris, n.metering_points_total  -- meters with, without, total
        FROM silver_norgespris_price_area_daily n               -- from Silver
        JOIN dim_consumption_group g ON g.consumption_group = n.consumption_group  -- consumption group key
        LEFT JOIN dim_price_area a ON a.price_area = n.price_area  -- price area key
    """)                                                        # end of the SQL


def build_agg_municipality_daily():                             # dashboard table: municipality x day
    """municipality x day: kWh per customer group, population, temperature and the ratios the dashboard needs."""
    return spark.sql("""                                        -- Spark SQL
        WITH c AS (                                    -- measured: customer groups side by side
            SELECT f.date_key, f.municipality_key,              -- per municipality and day:
                   SUM(CASE WHEN g.customer_group = 'private' THEN f.kwh END)                 AS kwh_private,  -- private kWh
                   SUM(CASE WHEN g.customer_group = 'business' THEN f.kwh END)                AS kwh_business,  -- business kWh
                   SUM(CASE WHEN g.customer_group = 'industry' THEN f.kwh END)                AS kwh_industry,  -- industry kWh
                   SUM(f.kwh)                                                                 AS kwh_all,  -- all kWh
                   SUM(CASE WHEN g.customer_group = 'private' THEN f.metering_points END)     AS metering_points_private,  -- private meters
                   SUM(CASE WHEN g.customer_group = 'business' THEN f.metering_points END)    AS metering_points_business,  -- business meters
                   SUM(CASE WHEN g.customer_group = 'industry' THEN f.metering_points END)    AS metering_points_industry,  -- industry meters
                   BOOL_AND(f.complete_day)                                                   AS complete_day  -- all groups complete?
            FROM fact_consumption_daily f                       -- from the consumption fact
            JOIN dim_customer_group g ON g.customer_group_key = f.customer_group_key  -- customer group names
            WHERE f.municipality_key <> -1                      -- known municipalities only
            GROUP BY f.date_key, f.municipality_key),           -- per municipality and day
        h AS (                                         -- allocated: household and cabin side by side
            SELECT f.date_key, f.municipality_key,              -- per municipality and day:
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.kwh END)                        AS kwh_household,  -- household kWh (allocated)
                   SUM(CASE WHEN g.consumption_group = 'cabin' THEN f.kwh END)                            AS kwh_cabin,  -- cabin kWh (allocated)
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.metering_points END)            AS metering_points_household,  -- household meters (allocated)
                   SUM(CASE WHEN g.consumption_group = 'cabin' THEN f.metering_points END)                AS metering_points_cabin,  -- cabin meters (allocated)
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.norgespris_metering_points END) AS norgespris_household,  -- household meters with Norgespris
                   SUM(CASE WHEN g.consumption_group = 'household' THEN f.counted_metering_points END)    AS counted_household  -- household meters counted
            FROM fact_private_split_daily f                     -- from the split fact
            JOIN dim_consumption_group g ON g.consumption_group_key = f.consumption_group_key  -- consumption group names
            GROUP BY f.date_key, f.municipality_key),           -- per municipality and day
        pop AS (                                       -- population for every year, from the newest year <= that year
            SELECT y.year, p.municipality_key, MAX_BY(p.population, p.year) AS population  -- newest population up to that year
            FROM (SELECT DISTINCT year FROM dim_date) y         -- every year of the calendar
            JOIN fact_population_yearly p ON p.year <= y.year   -- population of that year or before
            GROUP BY y.year, p.municipality_key)                -- per year and municipality
        SELECT c.municipality_key, c.date_key, d.date, d.norgespris_period,  -- keys and period
               c.kwh_private, c.kwh_business, c.kwh_industry, c.kwh_all,  -- kWh per customer group
               c.metering_points_private, c.metering_points_business, c.metering_points_industry,  -- meters per customer group
               h.kwh_household, h.kwh_cabin, h.metering_points_household, h.metering_points_cabin,  -- household and cabin
               p.population, w.mean_temp_c, w.hdd,              -- population and temperature
               ROUND(try_divide(CAST(c.kwh_private AS DOUBLE), c.metering_points_private), 4)     AS kwh_per_private_meter,  -- measured
               ROUND(try_divide(CAST(h.kwh_household AS DOUBLE), h.metering_points_household), 4) AS kwh_per_household,  -- allocated
               ROUND(try_divide(CAST(h.kwh_household AS DOUBLE), p.population), 4)                AS kwh_household_per_capita,  -- allocated
               ROUND(try_divide(CAST(h.kwh_cabin AS DOUBLE), h.metering_points_cabin), 4)         AS kwh_per_cabin,  -- allocated
               ROUND(try_divide(CAST(c.kwh_all AS DOUBLE), p.population), 4)                      AS kwh_all_per_capita,  -- measured
               ROUND(try_divide(CAST(c.kwh_business AS DOUBLE), c.metering_points_business), 4)   AS kwh_per_business_meter,  -- measured, control group
               h.norgespris_household, h.counted_household,     -- Norgespris counts
               ROUND(try_divide(CAST(h.norgespris_household AS DOUBLE), h.counted_household), 4)  AS norgespris_share_household,  -- share with Norgespris
               c.complete_day                                   -- quality flag
        FROM c                                                  -- the consumption per day
        JOIN dim_date d ON d.date_key = c.date_key              -- date attributes
        LEFT JOIN h ON h.date_key = c.date_key AND h.municipality_key = c.municipality_key  -- + household and cabin
        LEFT JOIN fact_weather_daily w ON w.date_key = c.date_key AND w.municipality_key = c.municipality_key  -- + temperature
        LEFT JOIN pop p ON p.municipality_key = c.municipality_key AND p.year = d.year  -- + population of that year
    """)                                                        # end of the SQL


def keep(df, name):                                             # compute now and keep as a view
    """Compute df now, keep the result and register it as view `name` (see 5.5: why localCheckpoint, not persist)."""
    df = df.localCheckpoint()                                   # materialise + cut the lineage (the plan becomes one scan)
    df.createOrReplaceTempView(name)                            # register under its name
    return df                                                   # the stored DataFrame


def build_area_views():                                         # the four steps of the temperature model
    """Register v_area, v_long, v_model and v_daily (see the markdown above); v_model and v_daily are materialised."""
    spark.sql("""                                               -- step 0: usable municipality-days
        CREATE OR REPLACE TEMP VIEW v_base AS                   -- view with municipality-days ...
        SELECT a.*, m.municipality_no, m.municipality_name, m.county_no, m.county_name, m.price_area, p.price_area_name  -- ... + names, county and price area
        FROM agg_municipality_daily a                           -- from the dashboard table
        JOIN dim_municipality m ON m.municipality_key = a.municipality_key  -- municipality attributes
        LEFT JOIN dim_price_area p ON p.price_area_key = m.price_area_key  -- price area name
        WHERE a.complete_day AND a.hdd IS NOT NULL AND a.population IS NOT NULL  -- only complete days with temperature and population
    """)                                                        # end of the SQL
    sums = """SUM(kwh_private) AS kwh_private, SUM(kwh_household) AS kwh_household, SUM(kwh_cabin) AS kwh_cabin,  -- sums used for every level (SQL text)
              SUM(kwh_business) AS kwh_business, SUM(kwh_all) AS kwh_all,  -- business and all kWh
              SUM(metering_points_private) AS mp_private, SUM(metering_points_household) AS mp_household,  -- private and household meters
              SUM(metering_points_cabin) AS mp_cabin, SUM(metering_points_business) AS mp_business,  -- cabin and business meters
              SUM(population) AS population,                    -- population
              SUM(norgespris_household) AS np_household, SUM(counted_household) AS counted_household,  -- Norgespris household counts
              SUM(CAST(hdd AS DOUBLE) * population) / SUM(population)         AS hdd,          -- population-weighted
              SUM(CAST(mean_temp_c AS DOUBLE) * population) / SUM(population) AS mean_temp_c"""  # temperature, population-weighted
    spark.sql(f"""                                              -- step 1: sums per level and day
        CREATE OR REPLACE TEMP VIEW v_area AS                   -- view with one row per level, area and day
        SELECT CASE WHEN grouping(county_no) = 0 THEN 'county'  -- level: county ...
                    WHEN grouping(price_area) = 0 THEN 'price_area' ELSE 'national' END     AS level,  -- ... price area or national
               CASE WHEN grouping(county_no) = 0 THEN county_no  -- area code: county number ...
                    WHEN grouping(price_area) = 0 THEN price_area ELSE 'NO' END             AS area_code,  -- ... price area or NO
               CASE WHEN grouping(county_no) = 0 THEN MAX(county_name)  -- area name: county name ...
                    WHEN grouping(price_area) = 0 THEN MAX(price_area_name) ELSE 'Norway' END AS area_name,  -- ... price area name or Norway
               date_key, {sums}                                 -- day + the sums
        FROM v_base                                             -- from step 0
        GROUP BY date_key, GROUPING SETS ((county_no), (price_area), ())  -- three levels in one pass
        UNION ALL                                               -- + the municipality level
        SELECT 'municipality', municipality_no, municipality_name, date_key, {sums}  -- municipality as its own area
        FROM v_base                                             -- from step 0
        GROUP BY municipality_no, municipality_name, date_key   -- one row per municipality and day
    """)                                                        # end of the SQL
    spark.sql("""                                               -- step 2: one row per measure
        SELECT level, area_code, area_name, date_key, hdd, mean_temp_c,  -- keys and temperature
               try_divide(CAST(np_household AS DOUBLE), counted_household) AS norgespris_share_household,  -- household share with Norgespris
               measure, value                                   -- measure name and value
        FROM v_area                                             -- from step 1
        LATERAL VIEW stack(6,                                   -- six measures -> six rows
            'kwh_per_private_meter',    try_divide(CAST(kwh_private AS DOUBLE), mp_private),  -- measured
            'kwh_per_household',        try_divide(CAST(kwh_household AS DOUBLE), mp_household),  -- allocated
            'kwh_household_per_capita', try_divide(CAST(kwh_household AS DOUBLE), population),  -- allocated
            'kwh_all_per_capita',       try_divide(CAST(kwh_all AS DOUBLE), population),  -- measured
            'kwh_per_cabin',            try_divide(CAST(kwh_cabin AS DOUBLE), mp_cabin),  -- allocated
            'kwh_per_business_meter',   try_divide(CAST(kwh_business AS DOUBLE), mp_business)) s AS measure, value  -- control group
        WHERE value IS NOT NULL                                 -- skip measures without a value
    """).createOrReplaceTempView("v_long")                    # not kept: read twice, from the kept agg_municipality_daily
    keep(spark.sql("""                                          -- step 3: the model before the start
        SELECT l.level, l.area_code, l.measure,                 -- per level, area and measure
               regr_intercept(l.value, l.hdd) AS intercept,      -- a: use on a day without heating
               regr_slope(l.value, l.hdd)     AS slope,          -- b: extra use per heating degree day
               regr_r2(l.value, l.hdd)        AS r2,             -- how much of the variation temperature explains
               COUNT(*)                       AS days_before,   -- days used for the model
               AVG(l.value)                   AS avg_before,    -- average before
               AVG(l.mean_temp_c)             AS mean_temp_before  -- average temperature before
        FROM v_long l JOIN dim_date d ON d.date_key = l.date_key  -- only real days
        WHERE d.norgespris_period = 'before'                    -- only days before 1 Oct 2025
        GROUP BY l.level, l.area_code, l.measure                -- one model per area and measure
    """), "v_model")                                            # end of the SQL, kept as v_model
    keep(spark.sql("""                                          -- step 4: expected value for every day
        SELECT l.*, d.date, d.norgespris_period, m.intercept + m.slope * l.hdd AS expected  -- actual row + expected = a + b * hdd
        FROM v_long l                                           -- from step 2
        JOIN dim_date d ON d.date_key = l.date_key              -- period of the day
        LEFT JOIN v_model m ON m.level = l.level AND m.area_code = l.area_code AND m.measure = l.measure  -- the model of this area and measure
    """), "v_daily")                                            # end of the SQL, kept as v_daily


def build_agg_area_daily():                                     # dashboard table: actual vs expected per day
    """Norway, price areas and counties: actual vs expected per day and measure."""
    return spark.sql("""                                        -- Spark SQL
        SELECT level, area_code, area_name, measure, date_key, date, norgespris_period,  -- keys and period
               ROUND(value, 4) AS value, ROUND(expected, 4) AS expected,  -- actual and expected
               ROUND(hdd, 2) AS hdd, ROUND(mean_temp_c, 2) AS mean_temp_c,  -- temperature
               ROUND(norgespris_share_household, 4) AS norgespris_share_household  -- Norgespris share
        FROM v_daily                                            -- from step 4
        WHERE level <> 'municipality'                           -- municipalities are in the effect table only
    """)                                                        # end of the SQL


def build_agg_norgespris_effect():                              # dashboard table: the answer per area and measure
    """One row per area and measure: the temperature-adjusted change after Norgespris (the answer to the question)."""
    keep(spark.sql(f"""                                         -- step 1: the effect
        WITH after AS (                                         -- after the start:
            SELECT level, area_code, measure, COUNT(*) AS days_after, AVG(value) AS avg_after,  -- days, averages ...
                   AVG(expected) AS avg_expected_after, SUM(value) AS sum_after, SUM(expected) AS sum_expected_after,  -- ... and sums
                   AVG(mean_temp_c) AS mean_temp_after          -- temperature after
            FROM v_daily WHERE norgespris_period = 'after'      -- only days after the start
            GROUP BY level, area_code, measure),                -- per area and measure
        names AS (SELECT DISTINCT level, area_code, area_name FROM v_daily),  -- area names
        latest AS (                                  -- Norgespris share of household meters on the newest day
            SELECT level, area_code, MAX_BY(norgespris_share_household, date_key) AS norgespris_share_latest  -- newest Norgespris share
            FROM v_daily WHERE norgespris_share_household IS NOT NULL GROUP BY level, area_code)  -- per area
        SELECT n.level, n.area_code, n.area_name, m.measure,    -- keys
               m.days_before, COALESCE(a.days_after, 0) AS days_after,  -- days before and after
               m.intercept, m.slope, m.r2,                      -- the model
               m.avg_before, a.avg_after, a.avg_expected_after,  -- averages
               100 * (try_divide(a.sum_after, a.sum_expected_after) - 1) AS adjusted_change_pct,  -- THE ANSWER: actual vs expected
               100 * (try_divide(a.avg_after, m.avg_before) - 1)        AS raw_change_pct,  -- simple before/after (for contrast)
               m.mean_temp_before, a.mean_temp_after, l.norgespris_share_latest,  -- temperatures and Norgespris share
               COALESCE(m.days_before >= {MIN_DAYS_BEFORE} AND a.days_after >= {MIN_DAYS_AFTER}  -- reliable if enough days ...
                        AND m.r2 >= {MIN_R2}, false)                    AS reliable  -- ... and a good model
        FROM v_model m                                          -- from the model
        JOIN names n ON n.level = m.level AND n.area_code = m.area_code  -- + names
        LEFT JOIN after a ON a.level = m.level AND a.area_code = m.area_code AND a.measure = m.measure  -- + the after values
        LEFT JOIN latest l ON l.level = m.level AND l.area_code = m.area_code  -- + the Norgespris share
    """), "v_effect")                                           # end of the SQL, kept as v_effect
    # second step: add the business control group (self-join on the stored view) and the ranking
    return spark.sql("""                                        -- step 2: control group and ranking
        SELECT e.level, e.area_code, e.area_name, e.measure, e.days_before, e.days_after,  -- keys and days
               ROUND(e.intercept, 4) AS intercept, ROUND(e.slope, 4) AS slope_per_hdd, ROUND(e.r2, 3) AS r2,  -- the model, rounded
               ROUND(e.avg_before, 4) AS avg_before, ROUND(e.avg_after, 4) AS avg_after,  -- averages, rounded
               ROUND(e.avg_expected_after, 4) AS avg_expected_after,  -- expected after, rounded
               ROUND(e.adjusted_change_pct, 2) AS adjusted_change_pct,  -- the answer, rounded
               ROUND(e.raw_change_pct, 2) AS raw_change_pct,    -- simple change, rounded
               ROUND(CASE WHEN e.measure <> 'kwh_per_business_meter'  -- change minus the business change ...
                          THEN e.adjusted_change_pct - b.adjusted_change_pct END, 2) AS adjusted_change_vs_business_pp,  -- ... in percentage points
               ROUND(e.mean_temp_before, 2) AS mean_temp_before, ROUND(e.mean_temp_after, 2) AS mean_temp_after,  -- temperatures
               ROUND(e.norgespris_share_latest, 4) AS norgespris_share_latest,  -- Norgespris share
               e.reliable,                                      -- reliable flag
               CASE WHEN e.reliable THEN ROW_NUMBER() OVER (PARTITION BY e.level, e.measure, e.reliable  -- rank by size of the change ...
                                                           ORDER BY ABS(e.adjusted_change_pct) DESC) END AS rank_abs_change  -- ... reliable rows only
        FROM v_effect e                                         -- the effect rows
        LEFT JOIN v_effect b ON b.level = e.level AND b.area_code = e.area_code AND b.measure = 'kwh_per_business_meter'  -- + the business row of the same area
    """)                                                        # end of the SQL


def build_agg_monthly_trend():                                  # dashboard table: monthly trend
    """Area x measure x month: averages, adjusted change of the month, year-over-year change."""
    return spark.sql("""                                        -- Spark SQL
        WITH m AS (                                             -- per month:
            SELECT level, area_code, area_name, measure, date_format(date, 'yyyy-MM') AS year_month,  -- keys and month
                   trunc(date, 'MM') AS month_start, COUNT(*) AS days,  -- first day of the month, days
                   AVG(value) AS avg_value, AVG(expected) AS avg_expected, AVG(hdd) AS avg_hdd, AVG(mean_temp_c) AS mean_temp_c,  -- averages
                   100 * (try_divide(SUM(value), SUM(expected)) - 1) AS adjusted_change_pct  -- adjusted change of the month
            FROM v_daily WHERE level <> 'municipality'          -- areas only
            GROUP BY level, area_code, area_name, measure, date_format(date, 'yyyy-MM'), trunc(date, 'MM'))  -- per area, measure and month
        SELECT m.level, m.area_code, m.area_name, m.measure, m.year_month, m.month_start, m.days,  -- keys and month
               CASE WHEN m.month_start >= DATE '{start}' THEN 'after' ELSE 'before' END AS norgespris_period,  -- before or after the start
               ROUND(m.avg_value, 4) AS avg_value, ROUND(m.avg_expected, 4) AS avg_expected,  -- averages, rounded
               ROUND(m.avg_hdd, 2) AS avg_hdd, ROUND(m.mean_temp_c, 2) AS mean_temp_c,  -- temperature, rounded
               ROUND(m.adjusted_change_pct, 2) AS adjusted_change_pct,  -- adjusted change, rounded
               ROUND(100 * (try_divide(m.avg_value, p.avg_value) - 1), 2) AS yoy_change_pct,  -- change vs same month last year
               ROUND(m.avg_hdd - p.avg_hdd, 2) AS yoy_hdd_diff  -- temperature difference vs last year
        FROM m                                                  -- this month
        LEFT JOIN m p ON p.level = m.level AND p.area_code = m.area_code AND p.measure = m.measure  -- the same month last year ...
                     AND p.month_start = add_months(m.month_start, -12)  -- ... 12 months earlier
    """.replace("{start}", str(NORGESPRIS_START)))              # fill in the start date


def build_agg_norgespris_uptake_daily():                        # dashboard table: Norgespris uptake
    """Day x price area x consumption group (after the start): share of meters and of kWh on Norgespris."""
    return spark.sql(f"""                                       -- Spark SQL
        WITH meters AS (                             -- Norgespris meters per municipality, summed per price area
            SELECT date_key, price_area_key, consumption_group_key,  -- meters with Norgespris per area and group
                   SUM(norgespris_metering_points) AS norgespris_metering_points,  -- with Norgespris
                   SUM(counted_metering_points) AS counted_metering_points  -- counted
            FROM fact_private_split_daily                       -- from the split fact
            WHERE date_key >= {NORGESPRIS_START:%Y%m%d} AND counted_metering_points IS NOT NULL  -- after the start, with counts
            GROUP BY date_key, price_area_key, consumption_group_key),  -- per day, area and group
        kwh AS (                                                -- kWh with Norgespris per area and group
            SELECT date_key, price_area_key, consumption_group_key,  -- keys
                   SUM(kwh_norgespris) AS kwh_norgespris, SUM(kwh_not_norgespris) AS kwh_not_norgespris  -- kWh with and without
            FROM fact_norgespris_price_area_daily               -- from the price-area fact
            WHERE date_key >= {NORGESPRIS_START:%Y%m%d}         -- after the start
            GROUP BY date_key, price_area_key, consumption_group_key)  -- per day, area and group
        SELECT date_key, price_area_key, consumption_group_key,  -- keys
               norgespris_metering_points, counted_metering_points,  -- meter counts
               ROUND(try_divide(CAST(norgespris_metering_points AS DOUBLE), counted_metering_points), 4) AS metering_point_share,  -- share of meters
               kwh_norgespris, kwh_not_norgespris,              -- kWh with and without
               ROUND(try_divide(CAST(kwh_norgespris AS DOUBLE), kwh_norgespris + kwh_not_norgespris), 4) AS kwh_share  -- share of kWh
        FROM meters FULL OUTER JOIN kwh USING (date_key, price_area_key, consumption_group_key)  -- both parts, matched on the keys
    """)                                                        # end of the SQL


TABLES = ["dim_date", "dim_municipality", "dim_customer_group", "dim_consumption_group",      # order = build + write order
          "dim_price_area",                                     # dimensions
          "fact_consumption_daily", "fact_private_split_daily", "fact_weather_daily", "fact_population_yearly",  # facts
          "fact_norgespris_price_area_daily",                   # facts
          "agg_municipality_daily", "agg_area_daily", "agg_norgespris_effect", "agg_monthly_trend",  # dashboard tables
          "agg_norgespris_uptake_daily"]                        # dashboard tables
KEEP = {"dim_date", "dim_municipality", "dim_customer_group", "dim_consumption_group",         # read again by later steps
        "dim_price_area",                                       # tables kept, continued
        "fact_consumption_daily", "agg_municipality_daily"}     # tables kept, continued


def date_bounds(silver):                                        # first and last day of all Silver data
    """(first day, last day) over all Silver dates, incl. 1 January of every population year."""
    parts = [silver[t].select(F.min("date").alias("lo"), F.max("date").alias("hi"))  # min and max date of ...
             for t in ("consumption_daily", "temperature_municipality_daily",  # ... these tables ...
                       "norgespris_count_daily", "norgespris_price_area_daily")]  # ... (the Norgespris ones may be empty)
    parts.append(silver["population_yearly"].select(F.expr("make_date(min(year), 1, 1)").alias("lo"),  # + 1 January of the first ...
                                                    F.expr("make_date(max(year), 1, 1)").alias("hi")))  # ... and last population year
    row = reduce(lambda a, b: a.unionByName(b), parts).agg(F.min("lo").alias("lo"), F.max("hi").alias("hi")).first()  # one min and one max over all
    return row["lo"], row["hi"]                                 # first day, last day


def build_gold_tables(silver):                                  # build every Gold table (lazy)
    """silver = {silver table name: DataFrame} -> {gold table name: DataFrame}, built lazily in dependency order."""
    for name, df in silver.items():                             # every Silver table ...
        df.createOrReplaceTempView(f"silver_{name}")             # e.g. silver_consumption_daily
    first_day, last_day = date_bounds(silver)                   # range of the calendar
    if first_day is None:                                       # no data at all
        raise ValueError("Silver has no dates - is silver.consumption_daily empty?")  # stop with a clear message
    consumption_groups = (silver["norgespris_count_daily"].select("consumption_group")       # Norgespris groups
                          .unionByName(silver["norgespris_price_area_daily"].select("consumption_group")))  # + groups of the price-area table
    builders = {                                                # table name -> function that builds it
        "dim_date": lambda: build_dim_date(first_day, last_day),  # calendar
        "dim_municipality": lambda: build_dim_municipality(silver["municipality"]),  # municipalities
        "dim_customer_group": lambda: build_dim_customer_group(silver["consumption_daily"]),  # customer groups
        "dim_consumption_group": lambda: build_dim_consumption_group(consumption_groups),  # consumption groups
        "dim_price_area": lambda: build_dim_price_area(silver["norgespris_price_area_daily"]),  # price areas
        "fact_consumption_daily": build_fact_consumption_daily,  # facts ...
        "fact_private_split_daily": build_fact_private_split_daily,  # household + cabin
        "fact_weather_daily": build_fact_weather_daily,         # temperature
        "fact_population_yearly": build_fact_population_yearly,  # population
        "fact_norgespris_price_area_daily": build_fact_norgespris_price_area_daily,  # Norgespris per price area
        "agg_municipality_daily": build_agg_municipality_daily,  # dashboard tables ...
        "agg_area_daily": build_agg_area_daily,                 # actual vs expected per day
        "agg_norgespris_effect": build_agg_norgespris_effect,   # the answer
        "agg_monthly_trend": build_agg_monthly_trend,           # monthly trend
        "agg_norgespris_uptake_daily": build_agg_norgespris_uptake_daily,  # Norgespris uptake
    }                                                           # end of the builders
    gold = {}                                                   # the built tables
    for table in TABLES:                                        # in dependency order
        if table == "agg_area_daily":                           # the area views need agg_municipality_daily
            build_area_views()                                  # build the four model views first
        df = builders[table]()                                  # build the table
        if table in KEEP:                                       # used again later?
            df = keep(df, table)                                # computed once now, reused by the next steps
        else:                                                   # used once
            df.createOrReplaceTempView(table)                   # later SQL can use the table by its name
        gold[table] = df                                        # remember it
    return gold                                                 # all Gold tables


PRIMARY_KEYS = {                                                # the grain of every Gold table
    "dim_date": ["date_key"],                                   # one row per day
    "dim_municipality": ["municipality_key"],                   # one row per municipality
    "dim_customer_group": ["customer_group_key"],               # one row per customer group
    "dim_consumption_group": ["consumption_group_key"],         # one row per consumption group
    "dim_price_area": ["price_area_key"],                       # one row per price area
    "fact_consumption_daily": ["date_key", "municipality_key", "customer_group_key"],  # municipality x day x customer group
    "fact_private_split_daily": ["date_key", "municipality_key", "consumption_group_key"],  # municipality x day x consumption group
    "fact_weather_daily": ["date_key", "municipality_key"],     # municipality x day
    "fact_population_yearly": ["municipality_key", "year"],     # municipality x year
    "fact_norgespris_price_area_daily": ["date_key", "price_area_key", "consumption_group_key", "consumption_band"],  # day x area x group x band
    "agg_municipality_daily": ["municipality_key", "date_key"],  # municipality x day
    "agg_area_daily": ["level", "area_code", "measure", "date_key"],  # level x area x measure x day
    "agg_norgespris_effect": ["level", "area_code", "measure"],  # level x area x measure
    "agg_monthly_trend": ["level", "area_code", "measure", "year_month"],  # level x area x measure x month
    "agg_norgespris_uptake_daily": ["date_key", "price_area_key", "consumption_group_key"],  # day x area x group
}                                                               # end of the grains
DIMENSION_OF = {"date_key": "dim_date", "municipality_key": "dim_municipality", "customer_group_key": "dim_customer_group",  # key column -> its dimension ...
                "consumption_group_key": "dim_consumption_group", "price_area_key": "dim_price_area"}  # ... continued
REFERENCES = {                                                  # table -> its key columns that point to a dimension
    "fact_consumption_daily": ["date_key", "municipality_key", "customer_group_key", "price_area_key"],  # keys of the consumption fact
    "fact_private_split_daily": ["date_key", "municipality_key", "consumption_group_key", "price_area_key"],  # keys of the split fact
    "fact_weather_daily": ["date_key", "municipality_key", "price_area_key"],  # keys of the weather fact
    "fact_population_yearly": ["date_key", "municipality_key", "price_area_key"],  # keys of the population fact
    "fact_norgespris_price_area_daily": ["date_key", "price_area_key", "consumption_group_key"],  # keys of the price-area fact
    "agg_municipality_daily": ["date_key", "municipality_key"],  # keys of the municipality table
    "agg_area_daily": ["date_key"],                             # key of the area table
    "agg_norgespris_uptake_daily": ["date_key", "price_area_key", "consumption_group_key"],  # keys of the uptake table
}                                                               # end of the references
FOREIGN_KEYS = [(table, column, DIMENSION_OF[column]) for table, columns in REFERENCES.items() for column in columns]  # (table, column, dimension) for every key


def gold_checks(s=STAGE):                                       # all checks as SQL pairs
    """[(check_id, severity, description, sql for the Gold number, sql for the expected number, rule), ...]"""
    checks = [                                                  # the fixed checks:
        ("G-01", "error", "kWh: fact_consumption_daily = silver.consumption_daily",  # G-01: total kWh ...
         f"SELECT SUM(kwh) FROM {s}.fact_consumption_daily", "SELECT SUM(kwh) FROM silver.consumption_daily", "equal"),  # ... Gold vs Silver
        ("G-02", "error", "rows with a known municipality: fact = silver",  # G-02: rows ...
         f"SELECT COUNT(*) FROM {s}.fact_consumption_daily WHERE municipality_key <> -1",  # ... in Gold ...
         "SELECT COUNT(*) FROM silver.consumption_daily WHERE known_municipality", "equal"),  # ... in Silver
        ("G-03", "error", "population: fact_population_yearly = silver.population_yearly",  # G-03: population ...
         f"SELECT SUM(population) FROM {s}.fact_population_yearly", "SELECT SUM(population) FROM silver.population_yearly",  # ... Gold vs Silver ...
         "equal"),                                              # ... must be equal
        ("G-04", "error", "municipality-days with temperature: fact_weather_daily = silver",  # G-04: temperature days ...
         f"SELECT COUNT(*) FROM {s}.fact_weather_daily",        # ... in Gold ...
         "SELECT COUNT(*) FROM silver.temperature_municipality_daily WHERE mean_temp_c IS NOT NULL", "equal"),  # ... in Silver
        ("G-05", "error", "kWh: agg_municipality_daily = known municipalities in the fact",  # G-05: kWh of the dashboard table ...
         f"SELECT SUM(kwh_all) FROM {s}.agg_municipality_daily",  # ... in the agg table ...
         f"SELECT SUM(kwh) FROM {s}.fact_consumption_daily WHERE municipality_key <> -1", "equal"),  # ... in the fact
        ("G-11", "error", "kWh: fact_private_split_daily = silver.private_split_daily (household + cabin)",  # G-11: household + cabin kWh ...
         f"SELECT SUM(kwh) FROM {s}.fact_private_split_daily",  # ... in Gold ...
         "SELECT SUM(kwh_household + kwh_cabin) FROM silver.private_split_daily", "equal"),  # ... in Silver
    ]                                                           # end of the fixed checks
    for table, columns in PRIMARY_KEYS.items():                 # G-06: one check per table
        cols = ", ".join(columns)                               # its key columns as SQL text
        checks.append((f"G-06 {table}", "error", f"grain is unique ({cols})",  # grain check ...
                       f"SELECT COUNT(*) FROM (SELECT 1 FROM {s}.{table} GROUP BY {cols} HAVING COUNT(*) > 1) x",  # ... count duplicate keys ...
                       "SELECT 0", "equal"))                    # ... must be 0
    for table, column, dim in FOREIGN_KEYS:                     # G-07: one check per foreign key
        checks.append((f"G-07 {table}.{column}", "error", f"every {column} exists in {dim}",  # orphan check ...
                       f"SELECT COUNT(*) FROM {s}.{table} f WHERE NOT EXISTS "  # ... count rows whose key ...
                       f"(SELECT 1 FROM {s}.{dim} d WHERE d.{column} = f.{column})", "SELECT 0", "equal"))  # ... is missing in the dimension
    checks += [                                                 # more checks:
        ("G-08", "error", "upstream: newest Silver run has 0 duplicate keys",  # G-08: upstream Silver run ...
         "SELECT COALESCE(SUM(value), 0) FROM silver.quality_metrics WHERE metric = 'duplicate_keys' "  # ... duplicate keys of ...
         "AND run_id = (SELECT MAX(run_id) FROM silver.quality_metrics)", "SELECT 0", "equal"),  # ... the newest Silver run
        ("G-09", "warning", "municipality-days without temperature",  # G-09: missing temperature (warning)
         f"SELECT COUNT(*) FROM {s}.agg_municipality_daily WHERE hdd IS NULL", "SELECT 0", "equal"),  # ... must be 0
        ("G-12", "warning", "kWh: household + cabin = measured private kWh (private kWh without a distribution key)",  # G-12: split = private (warning)
         f"SELECT SUM(kwh) FROM {s}.fact_private_split_daily",  # ... in Gold ...
         "SELECT SUM(kwh) FROM silver.consumption_daily WHERE customer_group = 'private' AND known_municipality", "equal"),  # ... measured in Silver
        ("G-10", "warning", "days since the newest consumption day (fresh data)",  # G-10: fresh data (warning)
         f"SELECT current_date - MAX(d.date) FROM {s}.fact_consumption_daily f "  # ... days since the newest day ...
         f"JOIN {s}.dim_date d ON d.date_key = f.date_key", "SELECT 45", "at_most"),  # ... at most 45
    ]                                                           # end of the list
    return checks                                               # all checks


def run_checks(run_id, s=STAGE):                                # run every check in PostgreSQL
    """Run every check in PostgreSQL; returns a list of result dicts."""
    results = []                                                # collected results
    for check_id, severity, description, sql_gold, sql_expected, rule in gold_checks(s):  # one check after the other
        try:                                                    # a failing query must not stop the run
            row = pg_query(f"SELECT CAST(({sql_gold}) AS DOUBLE PRECISION) AS g, "  # both numbers in one query ...
                           f"CAST(({sql_expected}) AS DOUBLE PRECISION) AS e").first()  # ... as one row
            g, e = row["g"], row["e"]                           # Gold value, expected value
            if rule == "equal":                                 # must be equal ...
                passed = g is not None and e is not None and abs(g - e) <= 0.01  # ... within 0.01
            else:                                               # at_most
                passed = g is not None and g <= e               # must not be larger
        except Exception as error:                              # e.g. a table is missing -> the check fails
            g, e, passed = None, None, False                    # no numbers, failed
            description = f"{description} (query failed: {str(error).splitlines()[0][:150]})"  # add the error to the text
        results.append({"run_id": run_id, "check_id": check_id, "severity": severity, "description": description,  # result row ...
                        "gold_value": g, "expected_value": e, "passed": passed})  # ... with the numbers
        if not passed:                                          # check failed?
            logger.warning(f"  check {check_id} ({severity}) FAILED: {description}: gold={g}, expected={e}")  # log it
    return results                                              # all results


def add_constraints(s=STAGE):                                   # add primary and foreign keys
    """Primary keys and foreign keys on the staged tables (one transaction)."""
    statements = [f"ALTER TABLE {s}.{t} ADD PRIMARY KEY ({', '.join(cols)})" for t, cols in PRIMARY_KEYS.items()]  # one PRIMARY KEY per table
    statements += [f"ALTER TABLE {s}.{t} ADD FOREIGN KEY ({c}) REFERENCES {s}.{dim} ({c})" for t, c, dim in FOREIGN_KEYS]  # one FOREIGN KEY per reference
    pg_execute(*statements)                                     # all in one transaction


def publish(tables):                                            # make the new version live
    """Swap gold_stage -> gold in one transaction; the tables that were live move to gold_previous."""
    live = {r["table_name"] for r in pg_query(                  # tables in gold now ...
        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{LIVE}'").collect()}  # ... from the catalog
    pg_execute(f"CREATE SCHEMA IF NOT EXISTS {LIVE}",           # in one transaction:
               f"DROP SCHEMA IF EXISTS {PREVIOUS} CASCADE",      # the version before the previous one is dropped
               f"CREATE SCHEMA {PREVIOUS}",                     # empty gold_previous
               *[f"ALTER TABLE {LIVE}.{t} SET SCHEMA {PREVIOUS}" for t in tables if t in live],  # current version -> gold_previous
               *[f"ALTER TABLE {STAGE}.{t} SET SCHEMA {LIVE}" for t in tables])  # new version -> gold


def log_run(run_id, status, silver_run_id, first_day, last_day, checks, rows, elapsed, message=""):  # write the run log and the summary file
    """Append one row to gold.publish_log and the check results to gold.check_results; write _gold_summary.md."""
    failed = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "error"]  # failed error checks
    warnings = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "warning"]  # failed warning checks
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")  # time of logging (UTC)
    log = spark.createDataFrame(                                # one log row ...
        [(run_id, now, ENV, status, silver_run_id, str(first_day or ""), str(last_day or ""),  # ... run, status, period ...
          len(checks), ", ".join(failed), ", ".join(warnings), message, float(elapsed))],  # ... checks, message, run time
        "run_id string, logged_at string, env string, status string, silver_run_id string, first_day string, "  # ... with these columns ...
        "last_day string, n_checks int, failed_checks string, warning_checks string, message string, elapsed_s double")  # ... continued
    pg_execute(f"CREATE SCHEMA IF NOT EXISTS {LIVE}")           # the log lives in gold
    log.write.jdbc(PG_URL, f"{LIVE}.publish_log", mode="append", properties=PG_PROPERTIES)  # append the log row
    if checks:                                                  # were checks run?
        (spark.createDataFrame([(c["run_id"], c["check_id"], c["severity"], c["description"], c["gold_value"],  # one row per check ...
                                 c["expected_value"], c["passed"]) for c in checks],  # ... with its numbers
                               "run_id string, check_id string, severity string, description string, "  # ... with these columns ...
                               "gold_value double, expected_value double, passed boolean")  # ... continued
              .write.jdbc(PG_URL, f"{LIVE}.check_results", mode="append", properties=PG_PROPERTIES))  # append them
    lines = [f"## Gold run {run_id} ({ENV}): **{status.upper()}**", "",  # markdown summary: title ...
             f"Data {first_day} .. {last_day} | Silver run {silver_run_id} | {elapsed:.0f}s | {message}", "",  # ... period, Silver run, time ...
             "| Table | Rows |", "|---|---|"] + [f"| {t} | {n:,} |" for t, n in rows.items()]  # ... one row per table
    lines += ["", "| Check | Severity | Gold | Expected | OK |", "|---|---|---|---|---|"]  # check table header
    lines += [f"| {c['check_id']} | {c['severity']} | {c['gold_value']} | {c['expected_value']} | "  # one row per check ...
              f"{'✅' if c['passed'] else '❌'} |" for c in checks]  # ... with OK or not
    os.makedirs(LANDING_DIR, exist_ok=True)                     # make sure the folder exists
    with open(os.path.join(LANDING_DIR, "_gold_summary.md"), "w", encoding="utf-8") as f:  # write the summary file ...
        f.write("\n".join(lines) + "\n")                        # ... as one text


def run_gold():                                                 # the whole Gold stage
    """Build, check and publish the Gold layer; returns a summary dict (same contract as the other stages)."""
    global spark                                                # the session used by the helpers
    start_time = time.time()                                    # start time
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):      # password missing?
        logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")  # clear message
        return {"status": "failed", "elapsed_time": 0.0}        # stop without starting Spark
    spark = get_spark()                                         # start Spark with the JDBC driver
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")  # id of this run (UTC time)
    available = silver_tables()                                 # tables in Silver
    required = ["municipality", "population_yearly", "consumption_daily", "temperature_municipality_daily",  # tables Gold cannot do without ...
                "private_split_daily"]                          # ... continued
    missing = [t for t in required if t not in available]       # which of them are missing
    silver_run_id = None                                        # newest Silver run, if known
    if "quality_metrics" in available:                          # Silver metrics exist?
        silver_run_id = pg_query("SELECT MAX(run_id) AS r FROM silver.quality_metrics").first()["r"]  # newest Silver run id
    if missing:                                                 # upstream incomplete -> keep the published version
        message = f"missing Silver tables {missing} - run the silver stage first"  # explain what to run first
        logger.error(message)                                   # log it
        log_run(run_id, "blocked", silver_run_id, None, None, [], {}, time.time() - start_time, message)  # log the blocked run
        return {"status": "failed", "elapsed_time": time.time() - start_time, "run_id": run_id}  # stop
    logger.info(f"Gold run {run_id} (env={ENV}, Silver run {silver_run_id}) -> {PG_URL}")  # log the start
    pg_execute(f"DROP SCHEMA IF EXISTS {STAGE} CASCADE", f"CREATE SCHEMA {STAGE}")   # a clean build area every run

    silver = {                                                  # the Silver inputs:
        "municipality": read_silver("municipality"),            # small table
        "population_yearly": read_silver("population_yearly"),  # small table
        "consumption_daily": read_silver("consumption_daily", by_month=True).persist(StorageLevel.MEMORY_AND_DISK),  # big: by month, kept
        "temperature_municipality_daily": read_silver("temperature_municipality_daily", by_month=True),  # by month
        "norgespris_count_daily": read_optional("norgespris_count_daily", available, by_month=True),  # optional, by month
        "norgespris_price_area_daily": read_optional("norgespris_price_area_daily", available),  # optional
        "private_split_daily": read_silver("private_split_daily", by_month=True),  # by month
    }                                                           # end of the inputs
    first_day, last_day = date_bounds(silver)                   # period of the data
    gold = build_gold_tables(silver)                            # build all tables
    rows, timings = {}, {}                                      # row counts and times per table
    for table in TABLES:                                        # write in dependency order
        t0 = time.time()                                        # start time of this table
        rows[table] = write_stage(gold[table], table)           # write it to gold_stage
        timings[table] = round(time.time() - t0, 1)             # seconds for this table

    checks = run_checks(run_id)                                 # run all checks
    failed = [c["check_id"] for c in checks if not c["passed"] and c["severity"] == "error"]  # failed error checks
    status, message = "published", ""                           # assume success
    if failed:                                                  # any error check failed?
        status, message = "blocked", f"{len(failed)} error check(s) failed - gold keeps the previous version"  # do not publish
    else:                                                       # all error checks passed
        try:                                                    # publishing may still fail
            add_constraints()                                   # keys in PostgreSQL
            publish(TABLES)                                     # swap into gold
        except Exception as error:                              # e.g. a constraint cannot be created
            status, message = "blocked", f"publishing failed: {str(error).splitlines()[0][:300]}"  # do not publish
    elapsed = time.time() - start_time                          # run time in seconds
    log_run(run_id, status, silver_run_id, first_day, last_day, checks, rows, elapsed, message)  # write the logs
    spark.catalog.clearCache()                                  # free the memory of every persisted table and view
    (logger.info if status == "published" else logger.error)(   # info if published, else error ...
        f"Gold {status} in {elapsed:.1f}s ({len(checks)} checks, failed: {failed or 'none'}) {message}")  # ... with the result
    return {"status": "success" if status == "published" else "failed", "elapsed_time": elapsed, "run_id": run_id,  # summary for run_pipeline.py ...
            "published": status == "published", "rows": rows, "timings_s": timings, "failed_checks": failed}  # ... with rows, times and failed checks


if __name__ == "__main__":                                      # only when run as a script
    result = run_gold()                                        # build, check and publish Gold
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
