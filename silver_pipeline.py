#!/usr/bin/env python3
"""
DAT535 Group 24 - Silver layer: bronze.* (PostgreSQL) -> silver.* (PostgreSQL)
===============================================================================

GENERATED from the code cells of notebooks/02_silver.ipynb (sections 2 and 4-5.1).
The notebook explains every line and the design choices; this file is what
GitHub Actions runs. If you change the logic, change the notebook cell and copy
it here, so the two stay identical.

Usage:
    python run_pipeline.py silver      (recommended; after "bronze" and "postgres")
    python silver_pipeline.py
"""

import logging                                                  # log messages (also visible in GitHub Actions)
import re                                                       # find year columns and municipality codes in SSB text
import time                                                     # elapsed time
from datetime import date, datetime, timedelta, timezone       # run id, timestamps (UTC) and the Silver period

from pyspark import StorageLevel                                # persist big inputs in memory, spill to disk if needed
from pyspark.sql import functions as F                          # built-in column functions
from pyspark.sql.window import Window                           # "newest version per key" (row_number over a window)

from bronze_pipeline import ENV, month_range, last_complete_month, next_month  # dev/prod + the period from .env
from bronze_to_postgres import get_spark, PG_URL, PG_PROPERTIES, PG_PASSWORD  # same Spark + database settings as Bronze

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver")                            # own logger name in the run log

spark = None                                                    # the SparkSession, set in run_silver() / the notebook
JDBC_READ = dict(PG_PROPERTIES, fetchsize="10000")              # stream 10 000 rows at a time (default: ALL rows into memory)


def pg_query(sql):
    """Run a SELECT inside PostgreSQL and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=JDBC_READ)


def bronze_tables():
    """Names of the tables that exist in the bronze schema."""
    rows = pg_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").collect()
    return {r["table_name"] for r in rows}


def read_bronze(table, split_on=None):
    """Read bronze.<table>; with split_on: one partition per month (YYYY-MM prefix of that column)."""
    if split_on is None:                                        # small table -> one partition is fine
        return spark.read.jdbc(PG_URL, f"bronze.{table}", properties=JDBC_READ)
    col = f'"{split_on}"'                                       # quoted: Postgres column names are upper case here
    months = [r["m"] for r in pg_query(f"SELECT DISTINCT left({col}, 7) AS m FROM bronze.{table}").collect()]
    months = sorted(m for m in months if m and re.fullmatch(r"\d{4}-\d{2}", m))  # only clean YYYY-MM values in SQL
    predicates = [f"left({col}, 7) = '{m}'" for m in months]    # one WHERE clause = one Spark partition
    predicates.append(f"{col} IS NULL OR left({col}, 7) !~ '^[0-9]{{4}}-[0-9]{{2}}$'")  # everything else (bad dates)
    return spark.read.jdbc(PG_URL, f"bronze.{table}", predicates=predicates, properties=JDBC_READ)


def write_silver(df, table):
    """Replace silver.<table> with df and return the number of rows now in PostgreSQL."""
    (df.write
       .option("batchsize", 10000)                              # rows per INSERT batch
       .jdbc(PG_URL, f"silver.{table}", mode="overwrite", properties=PG_PROPERTIES))
    rows = pg_query(f"SELECT COUNT(*) AS n FROM silver.{table}").first()["n"]  # count inside the database
    logger.info(f"  silver.{table}: {rows:,} rows")
    return rows


SILVER_SCHEMAS = {
    "municipality": [("municipality_no", "string"), ("municipality_name", "string"), ("county_no", "string")],
    "population_yearly": [("municipality_no", "string"), ("year", "int"), ("population", "int")],
    "consumption_daily": [("municipality_no", "string"), ("date", "date"), ("consumption_group", "string"),
                          ("kwh", "decimal(18,3)"), ("metering_points", "int"), ("n_hours", "int"),
                          ("complete_day", "boolean"), ("known_municipality", "boolean"),
                          ("last_update_time", "timestamp")],
    "weather_station": [("station_id", "string"), ("station_name", "string"), ("municipality_no", "string"),
                        ("municipality_match", "string"), ("county_no", "string"),
                        ("latitude", "double"), ("longitude", "double")],
    "temperature_station_daily": [("station_id", "string"), ("date", "date"), ("mean_temp_c", "decimal(5,2)"),
                                  ("time_offset", "string")],
    "temperature_municipality_daily": [("municipality_no", "string"), ("date", "date"),
                                       ("mean_temp_c", "decimal(5,2)"), ("n_stations", "int"), ("method", "string")],
    "norgespris_price_area_daily": [("date", "date"), ("price_area", "string"), ("consumption_group", "string"),
                                    ("eac_from_kwh", "decimal(18,3)"), ("eac_to_kwh", "decimal(18,3)"),
                                    ("kwh_norgespris", "decimal(18,3)"), ("kwh_not_norgespris", "decimal(18,3)"),
                                    ("kwh_total", "decimal(18,3)"), ("metering_points_norgespris", "int"),
                                    ("metering_points_not_norgespris", "int"), ("metering_points_total", "int")],
    "norgespris_count_daily": [("municipality_no", "string"), ("date", "date"), ("consumption_group", "string"),
                               ("metering_points_norgespris", "int"), ("metering_points_total", "int"),
                               ("norgespris_share", "decimal(6,4)"), ("known_municipality", "boolean")],
}

KEYS = {                                                        # the grain of every table (= what must be unique)
    "municipality": ["municipality_no"],
    "population_yearly": ["municipality_no", "year"],
    "consumption_daily": ["municipality_no", "date", "consumption_group"],
    "weather_station": ["station_id"],
    "temperature_station_daily": ["station_id", "date"],
    "temperature_municipality_daily": ["municipality_no", "date"],
    "norgespris_price_area_daily": ["date", "price_area", "consumption_group", "eac_from_kwh", "eac_to_kwh"],
    "norgespris_count_daily": ["municipality_no", "date", "consumption_group"],
}


def conform(df, table):
    """Exactly the columns of SILVER_SCHEMAS[table], in order, with the defined types."""
    return df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype in SILVER_SCHEMAS[table]])


def silver_period():
    """(first day, last day) of the period in .env - the same months the Frost download uses."""
    start, end = month_range()                                  # e.g. ('2024-01', None) in prod
    end = end or last_complete_month()                          # None = up to the last complete month
    first_day = date.fromisoformat(f"{start}-01")               # 2024-01 -> 2024-01-01
    last_day = date.fromisoformat(f"{next_month(end)}-01") - timedelta(days=1)  # 2026-10 -> 2026-10-31
    return first_day, last_day


def in_period(df, first_day, last_day):
    """Only the rows whose date lies in the period (both days included)."""
    return df.filter(F.col("date").between(first_day, last_day))


def try_cast(column, dtype):
    """Cast a column; NULL instead of an error when the value cannot be converted."""
    return F.expr(f"try_cast(`{column}` AS {dtype})")


def col_or_null(df, name, dtype="string"):
    """df[name] if the column exists, otherwise a NULL column of the given type."""
    return F.col(f"`{name}`") if name in df.columns else F.lit(None).cast(dtype)


def split_rejects(df, rules, dataset):
    """rules = [(rule_id, condition that is TRUE for a bad record), ...] -> (good_df, rejects_df)."""
    reason = F.lit(None).cast("string")
    for rule_id, is_bad in reversed(rules):                     # build "first rule that fails" from the back
        reason = F.when(F.coalesce(is_bad, F.lit(False)), F.lit(rule_id)).otherwise(reason)
    flagged = df.withColumn("_reject_rule", reason)
    good = flagged.filter(F.col("_reject_rule").isNull()).drop("_reject_rule")
    bad = flagged.filter(F.col("_reject_rule").isNotNull()).select(
        F.lit(dataset).alias("dataset"),                        # which Silver table it was meant for
        F.col("_reject_rule").alias("rule_id"),                 # which rule it broke
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns])).alias("record"))  # the record itself
    return good, bad


def newest_per_key(df, keys, order_cols):
    """One row per key: the first row when sorted by order_cols (use F.desc(...) for newest first)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")


def name_key(column):
    """Comparable name: upper case, only the part before ' - ' (SSB adds Sami/Kven names: 'Tromsø - Romsa - Tromssa')."""
    return F.upper(F.trim(F.regexp_replace(column, r"\s+-\s+.*$", "")))


def match_municipality(df, code_col, name_col, municipalities):
    """Add municipality_no + municipality_match ('code' / 'name' / None) using SSB's municipality list."""
    ref = municipalities.select(F.col("municipality_no").alias("_ref_no"),
                                name_key(F.col("municipality_name")).alias("_ref_name"))
    unique_names = (ref.groupBy("_ref_name").agg(F.count("*").alias("_n"), F.first("_ref_no").alias("_name_no"))
                       .filter("_n = 1").drop("_n"))            # names that point to exactly one municipality
    out = (df.join(F.broadcast(ref.select("_ref_no")), F.col(code_col) == F.col("_ref_no"), "left")
             .join(F.broadcast(unique_names), name_key(F.col(name_col)) == F.col("_ref_name"), "left"))
    return (out.withColumn("municipality_match",
                           F.when(F.col("_ref_no").isNotNull(), "code")
                            .when(F.col("_name_no").isNotNull(), "name"))
               .withColumn("municipality_no", F.coalesce("_ref_no", "_name_no", F.col(code_col)))
               .drop("_ref_no", "_ref_name", "_name_no"))


def clean_population(bronze):
    """bronze.population_07459 (wide) -> (population_yearly, municipality, rejects)."""
    region_cols = ([c for c in bronze.columns if re.search(r"region|kommune", c, re.IGNORECASE)]
                   or [bronze.columns[0]])                      # 'Kommune' (manual export) / 'Region - region' (API)
    year_of = {c: re.search(r"(19|20)\d{2}", c).group(0) for c in bronze.columns
               if c not in region_cols and not c.startswith("_") and re.search(r"(19|20)\d{2}", c)}  # column -> year
    years = sorted(set(year_of.values()))                       # '2023', '2024', ...
    pairs = F.array(*[F.struct(F.lit(int(y)).alias("year"),     # one (year, value) pair per year ...
                               F.coalesce(*[F.col(f"`{c}`") for c, cy in year_of.items() if cy == y]).alias("value"))
                      for y in years])                          # ... value from whichever column has that year
    region = F.coalesce(*[F.col(f"`{c}`") for c in region_cols])  # the region text, whichever format
    long = (bronze.select(region.alias("region"), "_landing_run_id", F.explode(pairs).alias("p"))
                  .select("region", "_landing_run_id", "p.year", "p.value"))  # one row per municipality and year
    df = (long.withColumn("municipality_no", F.regexp_extract("region", r"(\d{4})", 1))   # 'K-3101 - K-3101 Halden' -> '3101'
              .withColumn("municipality_name", F.trim(F.regexp_replace("region", r"^.*\d{4}\s*", "")))  # after the LAST code -> 'Halden'
              .withColumn("population", try_cast("value", "int")))
    good, rejects = split_rejects(df, [
        ("SSB-01", F.col("municipality_no") == ""),             # K-23 Kontinentalsokkelen, K-Rest ...
        ("SSB-02", F.col("population").isNull() | (F.col("population") <= 0)),
    ], "population_yearly")
    good = newest_per_key(good, ["municipality_no", "year"], [F.desc("_landing_run_id")])  # same year twice -> newest file
    municipality = (newest_per_key(good, ["municipality_no"], [F.desc("year")])  # name from the newest year
                    .withColumn("county_no", F.substring("municipality_no", 1, 2)))
    return conform(good, "population_yearly"), conform(municipality, "municipality"), rejects


def clean_consumption(bronze, municipalities):
    """bronze.consumption_per_group_municipality_hour -> (consumption_daily, rejects, number of duplicates removed)."""
    df = bronze.select(
        F.col("START_TIME").alias("start_time_text"),
        try_cast("START_TIME", "timestamp").alias("start_time"),              # instant in time (offset understood)
        F.expr("try_cast(substring(START_TIME, 1, 10) AS DATE)").alias("date"),  # LOCAL date, as written by Elhub
        F.lpad(F.trim("MUNICIPALITY_NUMBER"), 4, "0").alias("municipality_code"),  # '301' -> '0301', '0' -> '0000'
        F.col("NAME").alias("name"),
        F.lower(F.trim("CONSUMPTION_GROUP")).alias("consumption_group"),
        try_cast("QUANTITY_KWH", "decimal(18,3)").alias("kwh"),
        try_cast("METERING_POINT_COUNT", "int").alias("metering_points"),
        try_cast("LAST_UPDATE_TIME", "timestamp").alias("last_update_time"),
        F.col("_landing_run_id"))
    good, rejects = split_rejects(df, [
        ("CONS-01", F.col("start_time").isNull() | F.col("date").isNull()),
        ("CONS-02", F.col("kwh").isNull()),
        ("CONS-03", F.col("kwh") < 0),
        ("CONS-04", F.col("municipality_code").isNull() | (F.col("municipality_code") == "")),
        ("CONS-05", F.col("consumption_group").isNull() | (F.col("consumption_group") == "")),
    ], "consumption_daily")
    good = match_municipality(good, "municipality_code", "name", municipalities)  # old codes -> today's code
    deduped = newest_per_key(good, ["start_time", "municipality_no", "consumption_group"],
                             [F.desc_nulls_last("last_update_time"), F.desc("_landing_run_id")])
    daily = (deduped.groupBy("municipality_no", "date", "consumption_group")
                    .agg(F.sum("kwh").alias("kwh"),                          # kWh per local day
                         F.max("metering_points").alias("metering_points"),  # metering points that day
                         F.count("*").alias("n_hours"),                      # hours with data
                         F.max("last_update_time").alias("last_update_time"),
                         F.max(F.col("municipality_match").isNotNull()).alias("known_municipality"))
                    .withColumn("complete_day", F.col("n_hours").between(23, 25)))  # 23/25 = daylight saving days
    return conform(daily, "consumption_daily"), rejects, deduped


OBS_SCHEMA = "array<struct<elementId:string,value:double,timeOffset:string,qualityCode:int>>"  # Frost observation list


def clean_stations(bronze, municipalities):
    """bronze.frost_stations -> (weather_station, rejects)."""
    point = F.from_json(col_or_null(bronze, "geometry"), "struct<coordinates:array<double>>")  # {"coordinates":[lon,lat]}
    df = bronze.select(
        F.trim(col_or_null(bronze, "id")).alias("station_id"),
        col_or_null(bronze, "name").alias("station_name"),
        F.lpad(F.trim(col_or_null(bronze, "municipalityId")), 4, "0").alias("municipality_code"),  # 301 -> 0301
        col_or_null(bronze, "municipality").alias("frost_municipality_name"),
        point["coordinates"][1].alias("latitude"),
        point["coordinates"][0].alias("longitude"),
        F.col("_landing_run_id"))
    good, rejects = split_rejects(df, [("ST-01", F.col("station_id").isNull() | (F.col("station_id") == ""))],
                                  "weather_station")
    good = newest_per_key(good, ["station_id"], [F.desc("_landing_run_id")])
    good = match_municipality(good, "municipality_code", "frost_municipality_name", municipalities)
    good = (good.withColumn("municipality_no", F.when(F.col("municipality_match").isNotNull(), F.col("municipality_no")))
                .withColumn("county_no", F.substring("municipality_no", 1, 2)))  # unknown code -> no municipality
    return conform(good, "weather_station"), rejects


def clean_temperature(bronze):
    """bronze.frost_observations -> (temperature_station_daily, rejects)."""
    df = (bronze
          .select(F.split("sourceId", ":")[0].alias("station_id"),                       # 'SN18700:0' -> 'SN18700'
                  F.expr("to_date(convert_timezone('UTC', 'Europe/Oslo', "                   # UTC -> Norwegian time (CET/CEST)
                         "try_cast(substring(referenceTime, 1, 19) AS TIMESTAMP_NTZ)))").alias("date"),  # -> local date
                  F.explode(F.from_json("observations", OBS_SCHEMA)).alias("o"),          # one row per observation
                  "_landing_run_id")
          .select("station_id", "date", "_landing_run_id",
                  F.col("o.elementId").alias("element"), F.col("o.value").alias("value"),
                  F.col("o.timeOffset").alias("time_offset"))
          .filter(F.col("element") == "mean(air_temperature P1D)"))                       # only daily mean temperature
    good, rejects = split_rejects(df, [
        ("TEMP-01", F.col("date").isNull()),
        ("TEMP-02", F.col("value").isNull() | ~F.col("value").between(-60, 45)),
    ], "temperature_station_daily")
    good = newest_per_key(good, ["station_id", "date"],
                          [F.when(F.col("time_offset") == "PT0H", 0).otherwise(1),  # PT0H first ...
                           "time_offset",                                          # ... then the lowest offset
                           F.desc("_landing_run_id")])                             # ... newest file
    return conform(good.withColumnRenamed("value", "mean_temp_c"), "temperature_station_daily"), rejects


def temperature_per_municipality(temp_station, stations, municipalities):
    """Every municipality x day: average of own stations, else county average (Spark SQL)."""
    s = temp_station.sparkSession                               # the session these DataFrames belong to
    temp_station.createOrReplaceTempView("t_station")
    stations.createOrReplaceTempView("t_meta")
    municipalities.createOrReplaceTempView("t_muni")
    df = s.sql("""
        WITH obs AS (                                   -- temperature with the station's municipality and county
            SELECT t.date, t.mean_temp_c, m.municipality_no, m.county_no
            FROM t_station t JOIN t_meta m ON t.station_id = m.station_id
            WHERE m.municipality_no IS NOT NULL),
        own AS (SELECT municipality_no, date, AVG(mean_temp_c) AS temp, COUNT(*) AS n
                FROM obs GROUP BY municipality_no, date),
        county AS (SELECT county_no, date, AVG(mean_temp_c) AS temp, COUNT(*) AS n
                   FROM obs GROUP BY county_no, date),
        grid AS (SELECT m.municipality_no, m.county_no, d.date      -- every municipality x every day
                 FROM t_muni m CROSS JOIN (SELECT DISTINCT date FROM t_station) d)
        SELECT g.municipality_no, g.date,
               ROUND(COALESCE(o.temp, c.temp), 2)                       AS mean_temp_c,
               COALESCE(o.n, c.n)                                       AS n_stations,
               CASE WHEN o.temp IS NOT NULL THEN 'stations'
                    WHEN c.temp IS NOT NULL THEN 'county_average' END   AS method
        FROM grid g
        LEFT JOIN own o    ON o.municipality_no = g.municipality_no AND o.date = g.date
        LEFT JOIN county c ON c.county_no = g.county_no AND c.date = g.date
        WHERE COALESCE(o.temp, c.temp) IS NOT NULL                       -- no data at all -> no row
    """)
    return conform(df, "temperature_municipality_daily")


def clean_norgespris(bronze):
    """bronze.norgespris_consumption_per_group_eac_mba -> (norgespris_price_area_daily, rejects)."""
    def kwh(c):                                                 # text -> DECIMAL, '' -> NULL
        return F.expr(f"try_cast(nullif(trim(`{c}`), '') AS DECIMAL(18,3))")

    def count(c):                                               # '10702.0000000000' -> 10702
        return kwh(c).cast("int")

    df = bronze.select(
        F.expr("try_cast(substring(START_TIME, 1, 10) AS DATE)").alias("date"),   # local date
        F.trim("PRICE_AREA").alias("price_area"),
        F.lower(F.trim("CONSUMPTION_GROUP")).alias("consumption_group"),
        kwh("ESTIMATED_ANNUAL_CONSUMPTION_FROM").alias("eac_from_kwh"),
        kwh("ESTIMATED_ANNUAL_CONSUMPTION_TO").alias("eac_to_kwh"),
        kwh("QUANTITY_KWH_NORWAY_PRICE").alias("kwh_norgespris"),
        kwh("QUANTITY_KWH_NOT_NORWAY_PRICE").alias("kwh_not_norgespris"),
        kwh("NORGESPRIS_CONSUMPTION").alias("kwh_total"),
        count("METERING_POINT_COUNT_NORWAY_PRICE").alias("metering_points_norgespris"),
        count("METERING_POINT_COUNT_NOT_NORWAY_PRICE").alias("metering_points_not_norgespris"),
        count("METERING_POINT_COUNT").alias("metering_points_total"),
        try_cast("LAST_UPDATE_TIME", "timestamp").alias("last_update_time"),
        "_landing_run_id")
    good, rejects = split_rejects(df, [
        ("NP-01", F.col("date").isNull()),
        ("NP-02", F.col("price_area").isNull() | (F.col("price_area") == "")),
        ("NP-03", (F.col("kwh_norgespris") < 0) | (F.col("kwh_not_norgespris") < 0) | (F.col("kwh_total") < 0)),
    ], "norgespris_price_area_daily")
    good = newest_per_key(good, KEYS["norgespris_price_area_daily"],
                          [F.desc_nulls_last("last_update_time"), F.desc("_landing_run_id")])
    return conform(good, "norgespris_price_area_daily"), rejects


GROUP_NAMES = {"cabins and holiday properties": "cabin"}         # Elhub's long names -> names used in consumption_daily


def find_col(df, prefix, exact=False):
    """Name of the column that starts with (or, with exact=True, equals) prefix, ignoring upper/lower case."""
    for c in df.columns:
        if (c.upper() == prefix) if exact else c.upper().startswith(prefix):
            return c
    raise ValueError(f"No column {'named' if exact else 'starting with'} {prefix} in {df.columns}")


def standard_group(column):
    """Lower case, and Elhub's long group names mapped to the short ones (see GROUP_NAMES)."""
    g = F.lower(F.trim(column))
    for long_name, short_name in GROUP_NAMES.items():
        g = F.when(g == long_name, F.lit(short_name)).otherwise(g)
    return g


def clean_norgespris_count(bronze, municipalities):
    """bronze.norgespris_count_per_municipality_consumption_group -> (norgespris_count_daily, rejects)."""
    def count(c):                                               # '569' or '569.0000000000' -> 569
        return F.expr(f"try_cast(nullif(trim(`{c}`), '') AS DECIMAL(18,3))").cast("int")

    df = bronze.select(
        F.expr(f"try_cast(substring(`{find_col(bronze, 'DATE', exact=True)}`, 1, 10) AS DATE)").alias("date"),
        F.lpad(F.trim(F.col(f"`{find_col(bronze, 'MUNICIPALITY_NUM')}`")), 4, "0").alias("municipality_code"),
        F.col(f"`{find_col(bronze, 'MUNICIPALITY', exact=True)}`").alias("name"),
        standard_group(F.col(f"`{find_col(bronze, 'CONSUMPTION_GR')}`")).alias("consumption_group"),
        count(find_col(bronze, "COUNT_NORWAY")).alias("metering_points_norgespris"),
        count(find_col(bronze, "COUNT", exact=True)).alias("metering_points_total"),
        "_landing_run_id")
    good, rejects = split_rejects(df, [
        ("NPC-01", F.col("date").isNull()),
        ("NPC-02", F.col("municipality_code").isNull() | (F.col("municipality_code") == "")),
        ("NPC-03", F.col("metering_points_norgespris").isNull() | F.col("metering_points_total").isNull()
                   | (F.col("metering_points_norgespris") < 0)
                   | (F.col("metering_points_norgespris") > F.col("metering_points_total"))),
    ], "norgespris_count_daily")
    good = match_municipality(good, "municipality_code", "name", municipalities)   # old codes -> today's code
    good = newest_per_key(good, KEYS["norgespris_count_daily"], [F.desc("_landing_run_id")])  # same day twice -> newest
    good = (good.withColumn("known_municipality", F.col("municipality_match").isNotNull())
                .withColumn("norgespris_share",
                            F.when(F.col("metering_points_total") > 0,
                                   F.round(F.col("metering_points_norgespris") / F.col("metering_points_total"), 4))))
    return conform(good, "norgespris_count_daily"), rejects


def duplicate_keys(df, table):
    """Number of key values that occur more than once (must be 0 - proves the grain)."""
    return df.groupBy(*KEYS[table]).count().filter("count > 1").count()


def run_silver():
    """Build all Silver tables from the bronze schema; returns a summary dict (same contract as the other stages)."""
    global spark
    start_time = time.time()
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):
        logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")
        return {"status": "failed", "elapsed_time": 0.0}
    spark = get_spark()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    available = bronze_tables()
    required = ["population_07459", "consumption_per_group_municipality_hour", "frost_stations", "frost_observations"]
    missing = [t for t in required if t not in available]
    if missing:                                                 # Silver cannot be built without these
        logger.error(f"Missing Bronze tables {missing} - run the bronze and postgres stages first")
        return {"status": "failed", "elapsed_time": time.time() - start_time}
    first_day, last_day = silver_period()                       # the period from .env (same as the Frost download)
    logger.info(f"Silver run {run_id} (env={ENV}, period {first_day} .. {last_day}) -> {PG_URL}")
    metrics, rejects, rows = [], [], {}

    def metric(table, name, value):                             # collect one quality number
        metrics.append((run_id, table, name, float(value or 0)))

    # --- 4.1 municipalities and population ----------------------------------------------------
    pop_in = read_bronze("population_07459")
    population, municipality, rej = clean_population(pop_in)
    population = population.filter(F.col("year").between(first_day.year, last_day.year))  # years of the period
    rejects.append(rej)
    municipality = municipality.persist()                       # small, used by several steps below
    for table, df in (("population_yearly", population), ("municipality", municipality)):
        rows[table] = write_silver(df, table)
        metric(table, "silver_rows", rows[table])
        metric(table, "duplicate_keys", duplicate_keys(df, table))
    no_name = municipality.filter(F.col("municipality_name").isNull() | (F.col("municipality_name") == "")).count()
    metric("municipality", "municipalities_without_name", no_name)  # must be 0
    if no_name:                                                 # SSB delivered codes only -> check SSB_URL in Bronze
        logger.warning(f"  {no_name} municipalities without a name - does the SSB request ask for "
                       f"'usecodesandtexts' (SSB_URL in bronze_pipeline.py)?")
    metric("population_yearly", "bronze_rows", pop_in.count())
    metric("population_yearly", "rejected_rows", rej.count())

    # --- 4.2 consumption --------------------------------------------------------------------------
    cons_in = read_bronze("consumption_per_group_municipality_hour", split_on="START_TIME")
    cons_in = cons_in.persist(StorageLevel.MEMORY_AND_DISK)     # read PostgreSQL once, use it several times
    consumption, rej, hourly = clean_consumption(cons_in, municipality)
    consumption = in_period(consumption, first_day, last_day)   # only the days of the period
    rejects.append(rej)
    n_in, n_rej = cons_in.count(), rej.count()
    rows["consumption_daily"] = write_silver(consumption, "consumption_daily")
    metric("consumption_daily", "bronze_rows", n_in)
    metric("consumption_daily", "rejected_rows", n_rej)
    metric("consumption_daily", "duplicates_removed", n_in - n_rej - hourly.count())
    metric("consumption_daily", "silver_rows", rows["consumption_daily"])
    metric("consumption_daily", "duplicate_keys", duplicate_keys(consumption, "consumption_daily"))
    totals = consumption.agg(F.sum("kwh").alias("all"),
                             F.sum(F.when(~F.col("known_municipality"), F.col("kwh"))).alias("unknown"),
                             F.sum(F.when(~F.col("complete_day"), 1)).alias("incomplete")).first()
    metric("consumption_daily", "kwh_total", totals["all"])
    days = consumption.agg(F.min("date").alias("first"), F.max("date").alias("last")).first()
    metric("consumption_daily", "first_day_yyyymmdd", int(days["first"].strftime("%Y%m%d")) if days["first"] else 0)
    metric("consumption_daily", "last_day_yyyymmdd", int(days["last"].strftime("%Y%m%d")) if days["last"] else 0)
    metric("consumption_daily", "kwh_unknown_municipality", totals["unknown"])
    metric("consumption_daily", "incomplete_days", totals["incomplete"])
    cons_in.unpersist()

    # --- 4.3 weather -----------------------------------------------------------------------------
    st_in = read_bronze("frost_stations")
    stations, rej = clean_stations(st_in, municipality)
    rejects.append(rej)
    stations = stations.persist()
    rows["weather_station"] = write_silver(stations, "weather_station")
    for name, cond in (("stations_with_municipality", "municipality_no IS NOT NULL"),
                       ("stations_matched_by_name", "municipality_match = 'name'")):
        metric("weather_station", name, stations.filter(cond).count())
    metric("weather_station", "silver_rows", rows["weather_station"])

    obs_in = read_bronze("frost_observations", split_on="referenceTime").persist(StorageLevel.MEMORY_AND_DISK)
    temp_station, rej = clean_temperature(obs_in)
    temp_station = in_period(temp_station, first_day, last_day)  # (Frost already downloads only this period)
    rejects.append(rej)
    temp_station = temp_station.persist()
    rows["temperature_station_daily"] = write_silver(temp_station, "temperature_station_daily")
    metric("temperature_station_daily", "bronze_rows", obs_in.count())
    metric("temperature_station_daily", "rejected_rows", rej.count())
    metric("temperature_station_daily", "silver_rows", rows["temperature_station_daily"])
    metric("temperature_station_daily", "duplicate_keys", duplicate_keys(temp_station, "temperature_station_daily"))

    temp_muni = temperature_per_municipality(temp_station, stations, municipality)
    rows["temperature_municipality_daily"] = write_silver(temp_muni, "temperature_municipality_daily")
    for method in ("stations", "county_average"):              # how many municipalities use which method
        n = temp_muni.filter(F.col("method") == method).select("municipality_no").distinct().count()
        metric("temperature_municipality_daily", f"municipalities_{method}", n)
    metric("temperature_municipality_daily", "municipalities_without_temperature",
           municipality.join(temp_muni.select("municipality_no").distinct(), "municipality_no", "left_anti").count())
    metric("temperature_municipality_daily", "silver_rows", rows["temperature_municipality_daily"])
    obs_in.unpersist(); temp_station.unpersist(); stations.unpersist()

    # --- 4.4 Norgespris (optional) -----------------------------------------------------------------
    if "norgespris_consumption_per_group_eac_mba" in available:
        np_in = read_bronze("norgespris_consumption_per_group_eac_mba")
        norgespris, rej = clean_norgespris(np_in)
        norgespris = in_period(norgespris, first_day, last_day)
        rejects.append(rej)
        rows["norgespris_price_area_daily"] = write_silver(norgespris, "norgespris_price_area_daily")
        metric("norgespris_price_area_daily", "bronze_rows", np_in.count())
        metric("norgespris_price_area_daily", "rejected_rows", rej.count())
        metric("norgespris_price_area_daily", "silver_rows", rows["norgespris_price_area_daily"])
    else:
        logger.info("  norgespris_consumption_per_group_eac_mba not in Bronze - skipped (optional)")

    # --- 4.5 Norgespris meters per municipality (optional) ---------------------------------------------
    if "norgespris_count_per_municipality_consumption_group" in available:
        npc_in = read_bronze("norgespris_count_per_municipality_consumption_group", split_on="DATE")
        npc_in = npc_in.persist(StorageLevel.MEMORY_AND_DISK)
        norgespris_count, rej = clean_norgespris_count(npc_in, municipality)
        norgespris_count = in_period(norgespris_count, first_day, last_day)
        rejects.append(rej)
        rows["norgespris_count_daily"] = write_silver(norgespris_count, "norgespris_count_daily")
        metric("norgespris_count_daily", "bronze_rows", npc_in.count())
        metric("norgespris_count_daily", "rejected_rows", rej.count())
        metric("norgespris_count_daily", "silver_rows", rows["norgespris_count_daily"])
        metric("norgespris_count_daily", "duplicate_keys", duplicate_keys(norgespris_count, "norgespris_count_daily"))
        metric("norgespris_count_daily", "rows_unknown_municipality",
               norgespris_count.filter(~F.col("known_municipality")).count())
        npc_in.unpersist()
    else:
        logger.info("  norgespris_count_per_municipality_consumption_group not in Bronze - skipped (optional)")
    municipality.unpersist()

    # --- rejects and metrics -------------------------------------------------------------------------
    all_rejects = rejects[0]
    for r in rejects[1:]:
        all_rejects = all_rejects.unionByName(r)                 # same three columns in every rejects DataFrame
    all_rejects = all_rejects.select(F.lit(run_id).alias("run_id"), "*")
    rows["rejects"] = write_silver(all_rejects, "rejects")
    measured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    metrics_df = spark.createDataFrame([(r, measured_at, t, m, v) for r, t, m, v in metrics],
                                       "run_id string, measured_at string, table_name string, metric string, value double")
    metrics_df.write.jdbc(PG_URL, "silver.quality_metrics", mode="append", properties=PG_PROPERTIES)  # history

    elapsed = time.time() - start_time
    failed_keys = [m for m in metrics if m[2] == "duplicate_keys" and m[3] > 0]  # grain broken -> stage fails
    status = "failed" if failed_keys else "success"
    logger.info(f"Silver done in {elapsed:.1f}s: {rows}" + (f" DUPLICATE KEYS: {failed_keys}" if failed_keys else ""))
    return {"status": status, "elapsed_time": elapsed, "run_id": run_id, "rows": rows}


if __name__ == "__main__":
    result = run_silver()                                      # build all Silver tables
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
