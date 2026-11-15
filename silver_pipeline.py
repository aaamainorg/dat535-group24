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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # log format: time - logger - level - message
logger = logging.getLogger("silver")                            # own logger name in the run log

spark = None                                                    # the SparkSession, set in run_silver() / the notebook
JDBC_READ = dict(PG_PROPERTIES, fetchsize="10000")              # stream 10 000 rows at a time (default: ALL rows into memory)


def pg_query(sql):                                              # helper: SELECT inside PostgreSQL
    """Run a SELECT inside PostgreSQL and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=JDBC_READ)  # the query is wrapped as a subquery


def bronze_tables():                                            # which tables Bronze has
    """Names of the tables that exist in the bronze schema."""
    rows = pg_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").collect()  # ask PostgreSQL's catalog
    return {r["table_name"] for r in rows}                      # set of table names


def read_bronze(table, split_on=None):                          # read one Bronze table
    """Read bronze.<table>; with split_on: one partition per month (YYYY-MM prefix of that column)."""
    if split_on is None:                                        # small table -> one partition is fine
        return spark.read.jdbc(PG_URL, f"bronze.{table}", properties=JDBC_READ)  # one JDBC connection, one partition
    col = f'"{split_on}"'                                       # quoted: Postgres column names are upper case here
    months = [r["m"] for r in pg_query(f"SELECT DISTINCT left({col}, 7) AS m FROM bronze.{table}").collect()]  # the months in the data (YYYY-MM)
    months = sorted(m for m in months if m and re.fullmatch(r"\d{4}-\d{2}", m))  # only clean YYYY-MM values in SQL
    predicates = [f"left({col}, 7) = '{m}'" for m in months]    # one WHERE clause = one Spark partition
    predicates.append(f"{col} IS NULL OR left({col}, 7) !~ '^[0-9]{{4}}-[0-9]{{2}}$'")  # everything else (bad dates)
    return spark.read.jdbc(PG_URL, f"bronze.{table}", predicates=predicates, properties=JDBC_READ)  # parallel read: one WHERE per partition


def write_silver(df, table):                                    # write one Silver table
    """Replace silver.<table> with df and return the number of rows now in PostgreSQL."""
    (df.write                                                   # write ...
       .option("batchsize", 10000)                              # rows per INSERT batch
       .jdbc(PG_URL, f"silver.{table}", mode="overwrite", properties=PG_PROPERTIES))  # ... over JDBC, replacing the table
    rows = pg_query(f"SELECT COUNT(*) AS n FROM silver.{table}").first()["n"]  # count inside the database
    logger.info(f"  silver.{table}: {rows:,} rows")             # log the row count
    return rows                                                 # row count for the metrics


SILVER_SCHEMAS = {                                              # the columns + types of every Silver table
    "municipality": [("municipality_no", "string"), ("municipality_name", "string"), ("county_no", "string"),  # municipality: number, name, county ...
                     ("county_name", "string"), ("price_area", "string")],  # ... county name, main price area
    "population_yearly": [("municipality_no", "string"), ("year", "int"), ("population", "int")],  # population per municipality and year
    "consumption_daily": [("municipality_no", "string"), ("date", "date"), ("customer_group", "string"),  # consumption: key columns ...
                          ("kwh", "decimal(18,3)"), ("metering_points", "int"), ("n_hours", "int"),  # ... kWh, meters, hours with data ...
                          ("complete_day", "boolean"), ("known_municipality", "boolean"),  # ... quality flags ...
                          ("last_update_time", "timestamp")],   # ... newest Elhub update
    "weather_station": [("station_id", "string"), ("station_name", "string"), ("municipality_no", "string"),  # weather station: id, name, municipality ...
                        ("municipality_match", "string"), ("county_no", "string"),  # ... how it was matched, county ...
                        ("latitude", "double"), ("longitude", "double")],  # ... position
    "temperature_station_daily": [("station_id", "string"), ("date", "date"), ("mean_temp_c", "decimal(5,2)"),  # daily mean temperature per station ...
                                  ("time_offset", "string")],   # ... and which series was used
    "temperature_municipality_daily": [("municipality_no", "string"), ("date", "date"),  # temperature per municipality and day ...
                                       ("mean_temp_c", "decimal(5,2)"), ("n_stations", "int"), ("method", "string")],  # ... value, stations used, method
    "norgespris_price_area_daily": [("date", "date"), ("price_area", "string"), ("consumption_group", "string"),  # Norgespris per price area: keys ...
                                    ("eac_from_kwh", "decimal(18,3)"), ("eac_to_kwh", "decimal(18,3)"),  # ... consumption band ...
                                    ("kwh_norgespris", "decimal(18,3)"), ("kwh_not_norgespris", "decimal(18,3)"),  # ... kWh with / without Norgespris ...
                                    ("kwh_total", "decimal(18,3)"), ("metering_points_norgespris", "int"),  # ... total kWh, meters ...
                                    ("metering_points_not_norgespris", "int"), ("metering_points_total", "int")],  # ... meters
    "norgespris_count_daily": [("municipality_no", "string"), ("date", "date"), ("consumption_group", "string"),  # Norgespris meters per municipality: keys ...
                               ("metering_points_norgespris", "int"), ("metering_points_total", "int"),  # ... counts ...
                               ("norgespris_share", "decimal(6,4)"), ("known_municipality", "boolean")],  # ... share, quality flag
    "private_split_daily": [("municipality_no", "string"), ("date", "date"), ("price_area", "string"),  # distribution key private -> household + cabin: keys ...
                            ("kwh_private", "decimal(18,3)"), ("metering_points_private", "int"),  # ... private (measured) ...
                            ("household_meter_share", "double"), ("household_kwh_share", "double"),  # ... the two shares of the key ...
                            ("kwh_household", "decimal(18,3)"), ("kwh_cabin", "decimal(18,3)"),  # ... allocated kWh ...
                            ("metering_points_household", "int"), ("metering_points_cabin", "int"),  # ... allocated meters ...
                            ("norgespris_meters_household", "int"), ("counted_meters_household", "int"),  # ... Norgespris meters, household ...
                            ("norgespris_meters_cabin", "int"), ("counted_meters_cabin", "int"),  # ... Norgespris meters, cabin ...
                            ("norgespris_share_household", "decimal(6,4)"), ("norgespris_share_cabin", "decimal(6,4)"),  # ... Norgespris shares ...
                            ("norgespris_share_private", "decimal(6,4)"), ("key_method", "string")],  # ... and how the key was found
}                                                               # end of the schemas

KEYS = {                                                        # the grain of every table (= what must be unique)
    "municipality": ["municipality_no"],                        # one row per municipality
    "population_yearly": ["municipality_no", "year"],           # one row per municipality and year
    "consumption_daily": ["municipality_no", "date", "customer_group"],  # one row per municipality, day and group
    "weather_station": ["station_id"],                          # one row per station
    "temperature_station_daily": ["station_id", "date"],        # one row per station and day
    "temperature_municipality_daily": ["municipality_no", "date"],  # one row per municipality and day
    "norgespris_price_area_daily": ["date", "price_area", "consumption_group", "eac_from_kwh", "eac_to_kwh"],  # one row per day, area, group, band
    "norgespris_count_daily": ["municipality_no", "date", "consumption_group"],  # one row per municipality, day and group
    "private_split_daily": ["municipality_no", "date"],         # one row per municipality and day
}                                                               # end of the keys


def conform(df, table):                                         # make a DataFrame match its schema
    """Exactly the columns of SILVER_SCHEMAS[table], in order, with the defined types."""
    return df.select(*[F.col(name).cast(dtype).alias(name) for name, dtype in SILVER_SCHEMAS[table]])  # cast + order the columns


def silver_period():                                            # first and last day of the period
    """(first day, last day) of the period in .env - the same months the Frost download uses."""
    start, end = month_range()                                  # e.g. ('2024-01', None) in prod
    end = end or last_complete_month()                          # None = up to the last complete month
    first_day = date.fromisoformat(f"{start}-01")               # 2024-01 -> 2024-01-01
    last_day = date.fromisoformat(f"{next_month(end)}-01") - timedelta(days=1)  # 2026-10 -> 2026-10-31
    return first_day, last_day                                  # e.g. (2024-01-01, 2026-10-31)


def in_period(df, first_day, last_day):                         # keep only rows inside the period
    """Only the rows whose date lies in the period (both days included)."""
    return df.filter(F.col("date").between(first_day, last_day))  # between = both ends included


def try_cast(column, dtype):                                    # safe cast
    """Cast a column; NULL instead of an error when the value cannot be converted."""
    return F.expr(f"try_cast(`{column}` AS {dtype})")           # NULL instead of an error


def col_or_null(df, name, dtype="string"):                      # column or NULL
    """df[name] if the column exists, otherwise a NULL column of the given type."""
    return F.col(f"`{name}`") if name in df.columns else F.lit(None).cast(dtype)  # missing column -> NULL


def split_rejects(df, rules, dataset):                          # separate bad records from good ones
    """rules = [(rule_id, condition that is TRUE for a bad record), ...] -> (good_df, rejects_df)."""
    reason = F.lit(None).cast("string")                         # start: no reason
    for rule_id, is_bad in reversed(rules):                     # build "first rule that fails" from the back
        reason = F.when(F.coalesce(is_bad, F.lit(False)), F.lit(rule_id)).otherwise(reason)  # rule fails -> its id
    flagged = df.withColumn("_reject_rule", reason)             # add the reason column
    good = flagged.filter(F.col("_reject_rule").isNull()).drop("_reject_rule")  # good = no reason
    bad = flagged.filter(F.col("_reject_rule").isNotNull()).select(  # bad = with a reason ...
        F.lit(dataset).alias("dataset"),                        # which Silver table it was meant for
        F.col("_reject_rule").alias("rule_id"),                 # which rule it broke
        F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns])).alias("record"))  # the record itself
    return good, bad                                            # both DataFrames


def newest_per_key(df, keys, order_cols):                       # keep one row per key
    """One row per key: the first row when sorted by order_cols (use F.desc(...) for newest first)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)          # rows of the same key, in the given order
    return df.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")  # number them and keep number 1


def name_key(column):                                           # comparable municipality name
    """Comparable name: upper case, only the part before ' - ' (SSB adds Sami/Kven names: 'Tromsø - Romsa - Tromssa')."""
    return F.upper(F.trim(F.regexp_replace(column, r"\s+-\s+.*$", "")))  # cut at ' - ', trim, upper case


def match_municipality(df, code_col, name_col, municipalities):  # find today's municipality number
    """Add municipality_no + municipality_match ('code' / 'name' / None) using SSB's municipality list."""
    ref = municipalities.select(F.col("municipality_no").alias("_ref_no"),  # reference: number ...
                                name_key(F.col("municipality_name")).alias("_ref_name"))  # ... and comparable name
    unique_names = (ref.groupBy("_ref_name").agg(F.count("*").alias("_n"), F.first("_ref_no").alias("_name_no"))  # names that occur only once ...
                       .filter("_n = 1").drop("_n"))            # names that point to exactly one municipality
    out = (df.join(F.broadcast(ref.select("_ref_no")), F.col(code_col) == F.col("_ref_no"), "left")  # match on the number ...
             .join(F.broadcast(unique_names), name_key(F.col(name_col)) == F.col("_ref_name"), "left"))  # ... and on the name
    return (out.withColumn("municipality_match",                # how was it matched?
                           F.when(F.col("_ref_no").isNotNull(), "code")  # number found
                            .when(F.col("_name_no").isNotNull(), "name"))  # only the name found
               .withColumn("municipality_no", F.coalesce("_ref_no", "_name_no", F.col(code_col)))  # number, else number via name, else original
               .drop("_ref_no", "_ref_name", "_name_no"))       # drop the helper columns


COUNTIES = {                                                    # county number -> (name, main price area) - reference data
    "03": ("Oslo", "NO1"), "31": ("Østfold", "NO1"), "32": ("Akershus", "NO1"), "33": ("Buskerud", "NO1"),  # Oslo, Østfold, Akershus, Buskerud
    "34": ("Innlandet", "NO1"), "39": ("Vestfold", "NO1"), "40": ("Telemark", "NO2"), "42": ("Agder", "NO2"),  # Innlandet, Vestfold, Telemark, Agder
    "11": ("Rogaland", "NO2"), "46": ("Vestland", "NO5"), "15": ("Møre og Romsdal", "NO3"), "50": ("Trøndelag", "NO3"),  # Rogaland, Vestland, Møre og Romsdal, Trøndelag
    "18": ("Nordland", "NO4"), "55": ("Troms", "NO4"), "56": ("Finnmark", "NO4"),  # Nordland, Troms, Finnmark
}                                                               # end of the counties


def add_county(municipality):                                   # add county name + price area
    """+ county_name and the county's main price_area (unknown county -> 'Unknown county' / 'unknown')."""
    ref = municipality.sparkSession.createDataFrame([(no, name, area) for no, (name, area) in COUNTIES.items()],  # small reference table ...
                                                    "county_no string, county_name string, price_area string")  # ... with these columns
    return (municipality.join(F.broadcast(ref), "county_no", "left")  # join on the county number
            .withColumn("county_name", F.coalesce("county_name", F.lit("Unknown county")))  # unknown county -> text
            .withColumn("price_area", F.coalesce("price_area", F.lit("unknown"))))  # unknown county -> unknown area


def clean_population(bronze):                                   # SSB population (wide) -> long
    """bronze.population_07459 (wide) -> (population_yearly, municipality, rejects)."""
    region_cols = ([c for c in bronze.columns if re.search(r"region|kommune", c, re.IGNORECASE)]  # region column(s) by name ...
                   or [bronze.columns[0]])                      # 'Kommune' (manual export) / 'Region - region' (API)
    year_of = {c: re.search(r"(19|20)\d{2}", c).group(0) for c in bronze.columns  # find the year in the column names ...
               if c not in region_cols and not c.startswith("_") and re.search(r"(19|20)\d{2}", c)}  # column -> year
    years = sorted(set(year_of.values()))                       # '2023', '2024', ...
    pairs = F.array(*[F.struct(F.lit(int(y)).alias("year"),     # one (year, value) pair per year ...
                               F.coalesce(*[F.col(f"`{c}`") for c, cy in year_of.items() if cy == y]).alias("value"))  # value = first non-empty column of that year
                      for y in years])                          # ... value from whichever column has that year
    region = F.coalesce(*[F.col(f"`{c}`") for c in region_cols])  # the region text, whichever format
    long = (bronze.select(region.alias("region"), "_landing_run_id", F.explode(pairs).alias("p"))  # one row per region and year
                  .select("region", "_landing_run_id", "p.year", "p.value"))  # one row per municipality and year
    df = (long.withColumn("municipality_no", F.regexp_extract("region", r"(\d{4})", 1))   # 'K-3101 - K-3101 Halden' -> '3101'
              .withColumn("municipality_name", F.trim(F.regexp_replace("region", r"^.*\d{4}\s*", "")))  # after the LAST code -> 'Halden'
              .withColumn("population", try_cast("value", "int")))  # number or NULL
    good, rejects = split_rejects(df, [                         # apply the reject rules
        ("SSB-01", F.col("municipality_no") == ""),             # K-23 Kontinentalsokkelen, K-Rest ...
        ("SSB-02", F.col("population").isNull() | (F.col("population") <= 0)),  # missing or impossible population
    ], "population_yearly")                                     # dataset name for the rejects
    good = newest_per_key(good, ["municipality_no", "year"], [F.desc("_landing_run_id")])  # same year twice -> newest file
    municipality = (newest_per_key(good, ["municipality_no"], [F.desc("year")])  # name from the newest year
                    .withColumn("county_no", F.substring("municipality_no", 1, 2)))  # first two digits = county
    municipality = add_county(municipality)                     # + county name and main price area
    return conform(good, "population_yearly"), conform(municipality, "municipality"), rejects  # the three results


def clean_consumption(bronze, municipalities):                  # hourly Elhub -> daily Silver
    """bronze.consumption_per_group_municipality_hour -> (consumption_daily, rejects, number of duplicates removed)."""
    df = bronze.select(                                         # pick and type the columns
        F.col("START_TIME").alias("start_time_text"),           # original text (for the rejects)
        try_cast("START_TIME", "timestamp").alias("start_time"),              # instant in time (offset understood)
        F.expr("try_cast(substring(START_TIME, 1, 10) AS DATE)").alias("date"),  # LOCAL date, as written by Elhub
        F.lpad(F.trim("MUNICIPALITY_NUMBER"), 4, "0").alias("municipality_code"),  # '301' -> '0301', '0' -> '0000'
        F.col("NAME").alias("name"),                            # Elhub name (only for old codes)
        F.lower(F.trim("CONSUMPTION_GROUP")).alias("customer_group"),     # Elhub's name; it is the CUSTOMER group
        try_cast("QUANTITY_KWH", "decimal(18,3)").alias("kwh"),  # kWh as a decimal number
        try_cast("METERING_POINT_COUNT", "int").alias("metering_points"),  # number of meters
        try_cast("LAST_UPDATE_TIME", "timestamp").alias("last_update_time"),  # Elhub's update time
        F.col("_landing_run_id"))                               # lineage: which landing run
    good, rejects = split_rejects(df, [                         # apply the reject rules
        ("CONS-01", F.col("start_time").isNull() | F.col("date").isNull()),  # no valid time or date
        ("CONS-02", F.col("kwh").isNull()),                     # kWh missing
        ("CONS-03", F.col("kwh") < 0),                          # negative kWh
        ("CONS-04", F.col("municipality_code").isNull() | (F.col("municipality_code") == "")),  # no municipality
        ("CONS-05", F.col("customer_group").isNull() | (F.col("customer_group") == "")),  # no customer group
    ], "consumption_daily")                                     # dataset name for the rejects
    good = match_municipality(good, "municipality_code", "name", municipalities)  # old codes -> today's code
    deduped = newest_per_key(good, ["start_time", "municipality_no", "customer_group"],  # one row per hour, municipality, group ...
                             [F.desc_nulls_last("last_update_time"), F.desc("_landing_run_id")])  # ... the newest update wins
    daily = (deduped.groupBy("municipality_no", "date", "customer_group")  # sum the hours to the local day
                    .agg(F.sum("kwh").alias("kwh"),                          # kWh per local day
                         F.max("metering_points").alias("metering_points"),  # metering points that day
                         F.count("*").alias("n_hours"),                      # hours with data
                         F.max("last_update_time").alias("last_update_time"),  # newest Elhub update of the day
                         F.max(F.col("municipality_match").isNotNull()).alias("known_municipality"))  # true if the municipality is known
                    .withColumn("complete_day", F.col("n_hours").between(23, 25)))  # 23/25 = daylight saving days
    return conform(daily, "consumption_daily"), rejects, deduped  # daily table, rejects, hourly rows


OBS_SCHEMA = "array<struct<elementId:string,value:double,timeOffset:string,qualityCode:int>>"  # Frost observation list


def clean_stations(bronze, municipalities):                     # Frost stations -> weather_station
    """bronze.frost_stations -> (weather_station, rejects)."""
    point = F.from_json(col_or_null(bronze, "geometry"), "struct<coordinates:array<double>>")  # {"coordinates":[lon,lat]}
    df = bronze.select(                                         # pick and type the columns
        F.trim(col_or_null(bronze, "id")).alias("station_id"),  # station id
        col_or_null(bronze, "name").alias("station_name"),      # station name
        F.lpad(F.trim(col_or_null(bronze, "municipalityId")), 4, "0").alias("municipality_code"),  # 301 -> 0301
        col_or_null(bronze, "municipality").alias("frost_municipality_name"),  # municipality name from Frost
        point["coordinates"][1].alias("latitude"),              # latitude
        point["coordinates"][0].alias("longitude"),             # longitude
        F.col("_landing_run_id"))                               # lineage: which landing run
    good, rejects = split_rejects(df, [("ST-01", F.col("station_id").isNull() | (F.col("station_id") == ""))],  # reject stations without id ...
                                  "weather_station")            # ... dataset name
    good = newest_per_key(good, ["station_id"], [F.desc("_landing_run_id")])  # newest version per station
    good = match_municipality(good, "municipality_code", "frost_municipality_name", municipalities)  # today's municipality number
    good = (good.withColumn("municipality_no", F.when(F.col("municipality_match").isNotNull(), F.col("municipality_no")))  # unknown code -> no municipality ...
                .withColumn("county_no", F.substring("municipality_no", 1, 2)))  # unknown code -> no municipality
    return conform(good, "weather_station"), rejects            # stations and rejects


def clean_temperature(bronze):                                  # Frost observations -> daily mean per station
    """bronze.frost_observations -> (temperature_station_daily, rejects)."""
    df = (bronze                                                # from the Bronze table ...
          .select(F.split("sourceId", ":")[0].alias("station_id"),                       # 'SN18700:0' -> 'SN18700'
                  F.expr("to_date(convert_timezone('UTC', 'Europe/Oslo', "                   # UTC -> Norwegian time (CET/CEST)
                         "try_cast(substring(referenceTime, 1, 19) AS TIMESTAMP_NTZ)))").alias("date"),  # -> local date
                  F.explode(F.from_json("observations", OBS_SCHEMA)).alias("o"),          # one row per observation
                  "_landing_run_id")                            # lineage
          .select("station_id", "date", "_landing_run_id",      # flatten the observation ...
                  F.col("o.elementId").alias("element"), F.col("o.value").alias("value"),  # ... element and value ...
                  F.col("o.timeOffset").alias("time_offset"))   # ... and series
          .filter(F.col("element") == "mean(air_temperature P1D)"))                       # only daily mean temperature
    good, rejects = split_rejects(df, [                         # apply the reject rules
        ("TEMP-01", F.col("date").isNull()),                    # no valid date
        ("TEMP-02", F.col("value").isNull() | ~F.col("value").between(-60, 45)),  # missing or impossible temperature
    ], "temperature_station_daily")                             # dataset name for the rejects
    good = newest_per_key(good, ["station_id", "date"],         # one series per station and day ...
                          [F.when(F.col("time_offset") == "PT0H", 0).otherwise(1),  # PT0H first ...
                           "time_offset",                                          # ... then the lowest offset
                           F.desc("_landing_run_id")])                             # ... newest file
    return conform(good.withColumnRenamed("value", "mean_temp_c"), "temperature_station_daily"), rejects  # rename + conform, and the rejects


def temperature_per_municipality(temp_station, stations, municipalities):  # temperature per municipality and day
    """Every municipality x day: average of own stations, else county average (Spark SQL)."""
    s = temp_station.sparkSession                               # the session these DataFrames belong to
    temp_station.createOrReplaceTempView("t_station")           # the three inputs as SQL views
    stations.createOrReplaceTempView("t_meta")                  # station -> municipality
    municipalities.createOrReplaceTempView("t_muni")            # all municipalities
    df = s.sql("""                                              -- Spark SQL: own stations, else county
        WITH obs AS (                                   -- temperature with the station's municipality and county
            SELECT t.date, t.mean_temp_c, m.municipality_no, m.county_no  -- temperature + where the station is
            FROM t_station t JOIN t_meta m ON t.station_id = m.station_id  -- join the station's municipality
            WHERE m.municipality_no IS NOT NULL),               -- only stations inside a municipality
        own AS (SELECT municipality_no, date, AVG(mean_temp_c) AS temp, COUNT(*) AS n  -- average of own stations
                FROM obs GROUP BY municipality_no, date),       -- per municipality and day
        county AS (SELECT county_no, date, AVG(mean_temp_c) AS temp, COUNT(*) AS n  -- average of the county
                   FROM obs GROUP BY county_no, date),          -- per county and day
        grid AS (SELECT m.municipality_no, m.county_no, d.date      -- every municipality x every day
                 FROM t_muni m CROSS JOIN (SELECT DISTINCT date FROM t_station) d)  -- all days with data
        SELECT g.municipality_no, g.date,                       -- result columns
               ROUND(COALESCE(o.temp, c.temp), 2)                       AS mean_temp_c,  -- own value, else county value
               COALESCE(o.n, c.n)                                       AS n_stations,  -- how many stations were used
               CASE WHEN o.temp IS NOT NULL THEN 'stations'     -- which method was used ...
                    WHEN c.temp IS NOT NULL THEN 'county_average' END   AS method  -- ... county average
        FROM grid g                                             -- every municipality x day
        LEFT JOIN own o    ON o.municipality_no = g.municipality_no AND o.date = g.date  -- own stations
        LEFT JOIN county c ON c.county_no = g.county_no AND c.date = g.date  -- county average
        WHERE COALESCE(o.temp, c.temp) IS NOT NULL                       -- no data at all -> no row
    """)                                                        # end of the SQL
    return conform(df, "temperature_municipality_daily")        # exact Silver columns


def clean_norgespris(bronze):                                   # Norgespris per price area
    """bronze.norgespris_consumption_per_group_eac_mba -> (norgespris_price_area_daily, rejects)."""
    def kwh(c):                                                 # text -> DECIMAL, '' -> NULL
        return F.expr(f"try_cast(nullif(trim(`{c}`), '') AS DECIMAL(18,3))")  # text -> decimal, empty -> NULL

    def count(c):                                               # '10702.0000000000' -> 10702
        return kwh(c).cast("int")                               # decimal -> whole number

    df = bronze.select(                                         # pick and type the columns
        F.expr("try_cast(substring(START_TIME, 1, 10) AS DATE)").alias("date"),   # local date
        F.trim("PRICE_AREA").alias("price_area"),               # price area NO1-NO5
        F.lower(F.trim("CONSUMPTION_GROUP")).alias("consumption_group"),  # household / cabin
        kwh("ESTIMATED_ANNUAL_CONSUMPTION_FROM").alias("eac_from_kwh"),  # band: from (kWh per year)
        kwh("ESTIMATED_ANNUAL_CONSUMPTION_TO").alias("eac_to_kwh"),  # band: to (kWh per year)
        kwh("QUANTITY_KWH_NORWAY_PRICE").alias("kwh_norgespris"),  # kWh with Norgespris
        kwh("QUANTITY_KWH_NOT_NORWAY_PRICE").alias("kwh_not_norgespris"),  # kWh without Norgespris
        kwh("NORGESPRIS_CONSUMPTION").alias("kwh_total"),       # kWh in total
        count("METERING_POINT_COUNT_NORWAY_PRICE").alias("metering_points_norgespris"),  # meters with Norgespris
        count("METERING_POINT_COUNT_NOT_NORWAY_PRICE").alias("metering_points_not_norgespris"),  # meters without Norgespris
        count("METERING_POINT_COUNT").alias("metering_points_total"),  # meters in total
        try_cast("LAST_UPDATE_TIME", "timestamp").alias("last_update_time"),  # Elhub's update time
        "_landing_run_id")                                      # lineage
    good, rejects = split_rejects(df, [                         # apply the reject rules
        ("NP-01", F.col("date").isNull()),                      # no valid date
        ("NP-02", F.col("price_area").isNull() | (F.col("price_area") == "")),  # no price area
        ("NP-03", (F.col("kwh_norgespris") < 0) | (F.col("kwh_not_norgespris") < 0) | (F.col("kwh_total") < 0)),  # negative kWh
    ], "norgespris_price_area_daily")                           # dataset name for the rejects
    good = newest_per_key(good, KEYS["norgespris_price_area_daily"],  # one row per key ...
                          [F.desc_nulls_last("last_update_time"), F.desc("_landing_run_id")])  # ... the newest update wins
    return conform(good, "norgespris_price_area_daily"), rejects  # exact columns, and the rejects


GROUP_NAMES = {"cabins and holiday properties": "cabin"}         # Elhub's long names -> the short names of the price-area file


def find_col(df, prefix, exact=False):                          # find a column by its name start
    """Name of the column that starts with (or, with exact=True, equals) prefix, ignoring upper/lower case."""
    for c in df.columns:                                        # look at every column
        if (c.upper() == prefix) if exact else c.upper().startswith(prefix):  # exact name or prefix?
            return c                                            # found
    raise ValueError(f"No column {'named' if exact else 'starting with'} {prefix} in {df.columns}")  # clear error if it is missing


def standard_group(column):                                     # standard group name
    """Lower case, and Elhub's long group names mapped to the short ones (see GROUP_NAMES)."""
    g = F.lower(F.trim(column))                                 # lower case, no spaces around
    for long_name, short_name in GROUP_NAMES.items():           # replace long names by short ones
        g = F.when(g == long_name, F.lit(short_name)).otherwise(g)  # mapped name, else unchanged
    return g                                                    # the standard name


def clean_norgespris_count(bronze, municipalities):             # Norgespris meters per municipality
    """bronze.norgespris_count_per_municipality_consumption_group -> (norgespris_count_daily, rejects)."""
    def count(c):                                               # '569' or '569.0000000000' -> 569
        return F.expr(f"try_cast(nullif(trim(`{c}`), '') AS DECIMAL(18,3))").cast("int")  # text -> whole number

    df = bronze.select(                                         # pick and type the columns
        F.expr(f"try_cast(substring(`{find_col(bronze, 'DATE', exact=True)}`, 1, 10) AS DATE)").alias("date"),  # date
        F.lpad(F.trim(F.col(f"`{find_col(bronze, 'MUNICIPALITY_NUM')}`")), 4, "0").alias("municipality_code"),  # municipality number, 4 digits
        F.col(f"`{find_col(bronze, 'MUNICIPALITY', exact=True)}`").alias("name"),  # municipality name from Elhub
        standard_group(F.col(f"`{find_col(bronze, 'CONSUMPTION_GR')}`")).alias("consumption_group"),  # household / cabin
        count(find_col(bronze, "COUNT_NORWAY")).alias("metering_points_norgespris"),  # meters with Norgespris
        count(find_col(bronze, "COUNT", exact=True)).alias("metering_points_total"),  # meters in total
        "_landing_run_id")                                      # lineage
    good, rejects = split_rejects(df, [                         # apply the reject rules
        ("NPC-01", F.col("date").isNull()),                     # no valid date
        ("NPC-02", F.col("municipality_code").isNull() | (F.col("municipality_code") == "")),  # no municipality
        ("NPC-03", F.col("metering_points_norgespris").isNull() | F.col("metering_points_total").isNull()  # counts missing ...
                   | (F.col("metering_points_norgespris") < 0)  # ... negative ...
                   | (F.col("metering_points_norgespris") > F.col("metering_points_total"))),  # ... or more with Norgespris than in total
    ], "norgespris_count_daily")                                # dataset name for the rejects
    good = match_municipality(good, "municipality_code", "name", municipalities)   # old codes -> today's code
    good = newest_per_key(good, KEYS["norgespris_count_daily"], [F.desc("_landing_run_id")])  # same day twice -> newest
    good = (good.withColumn("known_municipality", F.col("municipality_match").isNotNull())  # known municipality?
                .withColumn("norgespris_share",                 # share with Norgespris ...
                            F.when(F.col("metering_points_total") > 0,  # ... only if there are meters ...
                                   F.round(F.col("metering_points_norgespris") / F.col("metering_points_total"), 4))))  # ... rounded to 4 decimals
    return conform(good, "norgespris_count_daily"), rejects     # exact columns, and the rejects


def split_private(consumption, municipality, count, price_area):  # distribution key: private -> household + cabin
    """Private kWh and meters per municipality x day -> household + cabin with a distribution key (Spark SQL)."""
    s = consumption.sparkSession                                # the Spark session of the inputs
    consumption.createOrReplaceTempView("k_consumption")        # inputs as SQL views
    municipality.createOrReplaceTempView("k_municipality")      # municipalities with price area
    count.createOrReplaceTempView("k_count")                    # Norgespris meter counts
    price_area.createOrReplaceTempView("k_price_area")          # Norgespris use per price area
    df = s.sql("""                                              -- Spark SQL, step by step
        WITH private AS (                     -- the Norgespris-eligible customer group, known municipalities
            SELECT c.municipality_no, c.date, m.price_area,     -- municipality, day, price area ...
                   c.kwh AS kwh_private, c.metering_points AS metering_points_private  -- ... private kWh and meters
            FROM k_consumption c JOIN k_municipality m ON m.municipality_no = c.municipality_no  -- price area of the municipality
            WHERE c.customer_group = 'private' AND c.known_municipality),  -- only private, known municipalities
        use_day AS (                          -- kWh per meter per day for households and cabins, per price area
            SELECT price_area, date,                            -- per price area and day:
                   try_divide(SUM(CASE WHEN consumption_group = 'household' THEN CAST(kwh_total AS DOUBLE) END),  -- household kWh ...
                              SUM(CASE WHEN consumption_group = 'household' THEN metering_points_total END)) AS kwh_per_household,  -- ... per household meter
                   try_divide(SUM(CASE WHEN consumption_group = 'cabin' THEN CAST(kwh_total AS DOUBLE) END),  -- cabin kWh ...
                              SUM(CASE WHEN consumption_group = 'cabin' THEN metering_points_total END))     AS kwh_per_cabin  -- ... per cabin meter
            FROM k_price_area GROUP BY price_area, date),       -- per price area and day
        use_month AS (                        -- fallback for a missing day: average of the same calendar month
            SELECT price_area, month(date) AS month,            -- per price area and calendar month:
                   AVG(kwh_per_household) AS kwh_per_household, AVG(kwh_per_cabin) AS kwh_per_cabin  -- average use per meter
            FROM use_day GROUP BY price_area, month(date)),     -- per price area and month
        mix_municipality AS (                 -- household share of the municipality's household + cabin meters
            SELECT municipality_no,                             -- per municipality:
                   try_divide(SUM(CASE WHEN consumption_group = 'household' THEN CAST(metering_points_total AS DOUBLE) END),  -- household meters ...
                              SUM(metering_points_total)) AS household_meter_share  -- ... per household + cabin meters
            FROM k_count WHERE known_municipality AND consumption_group IN ('household', 'cabin')  -- only these two groups
            GROUP BY municipality_no),                          -- per municipality
        mix_price_area AS (                   -- fallback: the same share for the whole price area
            SELECT price_area,                                  -- per price area:
                   try_divide(SUM(CASE WHEN consumption_group = 'household' THEN CAST(metering_points_total AS DOUBLE) END),  -- household meters ...
                              SUM(metering_points_total)) AS household_meter_share  -- ... per household + cabin meters
            FROM k_price_area WHERE consumption_group IN ('household', 'cabin') GROUP BY price_area),  -- per price area
        meters AS (                           -- Norgespris meters per municipality and day, household and cabin side by side
            SELECT municipality_no, date,                       -- per municipality and day:
                   SUM(CASE WHEN consumption_group = 'household' THEN metering_points_norgespris END) AS np_household,  -- household meters with Norgespris
                   SUM(CASE WHEN consumption_group = 'household' THEN metering_points_total END)      AS counted_household,  -- household meters counted
                   SUM(CASE WHEN consumption_group = 'cabin' THEN metering_points_norgespris END)     AS np_cabin,  -- cabin meters with Norgespris
                   SUM(CASE WHEN consumption_group = 'cabin' THEN metering_points_total END)          AS counted_cabin  -- cabin meters counted
            FROM k_count WHERE known_municipality GROUP BY municipality_no, date),  -- per municipality and day
        keyed AS (                                              -- all inputs of the key per row
            SELECT p.*,                                         -- the private row ...
                   COALESCE(mm.household_meter_share, mp.household_meter_share) AS s,          -- meter mix
                   CASE WHEN mm.household_meter_share IS NOT NULL THEN 'municipality'  -- where the meter mix came from ...
                        WHEN mp.household_meter_share IS NOT NULL THEN 'price_area' END AS mix_from,  -- ... price area fallback
                   COALESCE(d.kwh_per_household, mo.kwh_per_household) AS ih,                 -- use per household meter
                   COALESCE(d.kwh_per_cabin, mo.kwh_per_cabin) AS ic                           -- use per cabin meter
            FROM private p                                      -- private consumption
            LEFT JOIN mix_municipality mm ON mm.municipality_no = p.municipality_no  -- + meter mix of the municipality
            LEFT JOIN mix_price_area mp ON mp.price_area = p.price_area  -- + meter mix of the price area
            LEFT JOIN use_day d ON d.price_area = p.price_area AND d.date = p.date  -- + use per meter that day
            LEFT JOIN use_month mo ON mo.price_area = p.price_area AND mo.month = month(p.date)),  -- + use per meter that month
        shared AS (                           -- household share of the kWh: s*ih / (s*ih + (1-s)*ic)
            SELECT k.*, COALESCE(try_divide(k.s * k.ih, k.s * k.ih + (1 - k.s) * k.ic), k.s) AS household_kwh_share  -- + household kWh share
            FROM keyed k)                                       -- from the keyed rows
        SELECT x.municipality_no, x.date, x.price_area, x.kwh_private, x.metering_points_private,  -- result: keys and private values
               x.s AS household_meter_share, x.household_kwh_share,  -- the two shares
               CAST(x.kwh_private * x.household_kwh_share AS DECIMAL(18,3))                     AS kwh_household,  -- household kWh
               x.kwh_private - CAST(x.kwh_private * x.household_kwh_share AS DECIMAL(18,3))     AS kwh_cabin,  -- cabin kWh = the rest
               CAST(ROUND(x.metering_points_private * x.s) AS INT)                               AS metering_points_household,  -- household meters
               x.metering_points_private - CAST(ROUND(x.metering_points_private * x.s) AS INT)   AS metering_points_cabin,  -- cabin meters = the rest
               n.np_household AS norgespris_meters_household, n.counted_household AS counted_meters_household,  -- household meter counts
               n.np_cabin AS norgespris_meters_cabin, n.counted_cabin AS counted_meters_cabin,  -- cabin meter counts
               ROUND(try_divide(CAST(n.np_household AS DOUBLE), n.counted_household), 4)         AS norgespris_share_household,  -- share with Norgespris, household
               ROUND(try_divide(CAST(n.np_cabin AS DOUBLE), n.counted_cabin), 4)                 AS norgespris_share_cabin,  -- share with Norgespris, cabin
               ROUND(try_divide(CAST(COALESCE(n.np_household, 0) + COALESCE(n.np_cabin, 0) AS DOUBLE),  -- share with Norgespris, private ...
                                COALESCE(n.counted_household, 0) + COALESCE(n.counted_cabin, 0)), 4) AS norgespris_share_private,  -- ... both groups together
               CASE WHEN x.s IS NULL THEN NULL                  -- how the key was found:
                    WHEN x.ih IS NULL OR x.ic IS NULL THEN concat(x.mix_from, ' (meters only)')  -- meter mix only
                    ELSE x.mix_from END                                                          AS key_method  -- municipality / price area
        FROM shared x                                           -- the private rows with their key
        LEFT JOIN meters n ON n.municipality_no = x.municipality_no AND n.date = x.date  -- + Norgespris meter counts
    """)                                                        # end of the SQL
    return conform(df, "private_split_daily")                   # exact Silver columns


def duplicate_keys(df, table):                                  # how many keys occur more than once
    """Number of key values that occur more than once (must be 0 - proves the grain)."""
    return df.groupBy(*KEYS[table]).count().filter("count > 1").count()  # must be 0


def run_silver():                                               # the whole Silver stage
    """Build all Silver tables from the bronze schema; returns a summary dict (same contract as the other stages)."""
    global spark                                                # the session used by the helpers
    start_time = time.time()                                    # start time
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):      # password missing?
        logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")  # clear message
        return {"status": "failed", "elapsed_time": 0.0}        # stop without starting Spark
    spark = get_spark()                                         # start Spark with the JDBC driver
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")  # id of this run (UTC time)
    available = bronze_tables()                                 # tables in Bronze
    required = ["population_07459", "consumption_per_group_municipality_hour", "frost_stations", "frost_observations"]  # tables Silver cannot do without
    missing = [t for t in required if t not in available]       # which of them are missing
    if missing:                                                 # Silver cannot be built without these
        logger.error(f"Missing Bronze tables {missing} - run the bronze and postgres stages first")  # explain what to run first
        return {"status": "failed", "elapsed_time": time.time() - start_time}  # stop
    first_day, last_day = silver_period()                       # the period from .env (same as the Frost download)
    logger.info(f"Silver run {run_id} (env={ENV}, period {first_day} .. {last_day}) -> {PG_URL}")  # log the start
    metrics, rejects, rows = [], [], {}                         # collected metrics, rejects, row counts

    def metric(table, name, value):                             # collect one quality number
        metrics.append((run_id, table, name, float(value or 0)))  # add one metric row

    # --- 4.1 municipalities and population ----------------------------------------------------
    pop_in = read_bronze("population_07459")                    # read the population table
    population, municipality, rej = clean_population(pop_in)    # clean it
    population = population.filter(F.col("year").between(first_day.year, last_day.year))  # years of the period
    rejects.append(rej)                                         # collect its rejects
    municipality = municipality.persist()                       # small, used by several steps below
    for table, df in (("population_yearly", population), ("municipality", municipality)):  # write both tables ...
        rows[table] = write_silver(df, table)                   # ... to PostgreSQL
        metric(table, "silver_rows", rows[table])               # rows written
        metric(table, "duplicate_keys", duplicate_keys(df, table))  # must be 0
    no_name = municipality.filter(F.col("municipality_name").isNull() | (F.col("municipality_name") == "")).count()  # municipalities without a name
    metric("municipality", "municipalities_without_name", no_name)  # must be 0
    if no_name:                                                 # SSB delivered codes only -> check SSB_URL in Bronze
        logger.warning(f"  {no_name} municipalities without a name - does the SSB request ask for "  # probably codes only from SSB ...
                       f"'usecodesandtexts' (SSB_URL in bronze_pipeline.py)?")  # ... hint where to fix it
    metric("population_yearly", "bronze_rows", pop_in.count())  # rows read from Bronze
    metric("population_yearly", "rejected_rows", rej.count())   # rows rejected

    # --- 4.2 consumption --------------------------------------------------------------------------
    cons_in = read_bronze("consumption_per_group_municipality_hour", split_on="START_TIME")  # read the hourly table, one partition per month
    cons_in = cons_in.persist(StorageLevel.MEMORY_AND_DISK)     # read PostgreSQL once, use it several times
    consumption, rej, hourly = clean_consumption(cons_in, municipality)  # clean and sum to days
    consumption = in_period(consumption, first_day, last_day)   # only the days of the period
    rejects.append(rej)                                         # collect its rejects
    n_in, n_rej = cons_in.count(), rej.count()                  # rows read, rows rejected
    rows["consumption_daily"] = write_silver(consumption, "consumption_daily")  # write consumption_daily
    metric("consumption_daily", "bronze_rows", n_in)            # rows read from Bronze
    metric("consumption_daily", "rejected_rows", n_rej)         # rows rejected
    metric("consumption_daily", "duplicates_removed", n_in - n_rej - hourly.count())  # removed as older versions
    metric("consumption_daily", "silver_rows", rows["consumption_daily"])  # rows written
    metric("consumption_daily", "duplicate_keys", duplicate_keys(consumption, "consumption_daily"))  # must be 0
    totals = consumption.agg(F.sum("kwh").alias("all"),         # totals: all kWh ...
                             F.sum(F.when(~F.col("known_municipality"), F.col("kwh"))).alias("unknown"),  # ... kWh of unknown municipalities ...
                             F.sum(F.when(~F.col("complete_day"), 1)).alias("incomplete")).first()  # ... days with missing hours
    metric("consumption_daily", "kwh_total", totals["all"])     # all kWh
    days = consumption.agg(F.min("date").alias("first"), F.max("date").alias("last")).first()  # first and last day
    metric("consumption_daily", "first_day_yyyymmdd", int(days["first"].strftime("%Y%m%d")) if days["first"] else 0)  # first day as a number
    metric("consumption_daily", "last_day_yyyymmdd", int(days["last"].strftime("%Y%m%d")) if days["last"] else 0)  # last day as a number
    metric("consumption_daily", "kwh_unknown_municipality", totals["unknown"])  # kWh of unknown municipalities
    metric("consumption_daily", "incomplete_days", totals["incomplete"])  # days with missing hours
    cons_in.unpersist()                                         # free the memory

    # --- 4.3 weather -----------------------------------------------------------------------------
    st_in = read_bronze("frost_stations")                       # read the station table
    stations, rej = clean_stations(st_in, municipality)         # clean it
    rejects.append(rej)                                         # collect its rejects
    stations = stations.persist()                               # keep it: used several times
    rows["weather_station"] = write_silver(stations, "weather_station")  # write weather_station
    for name, cond in (("stations_with_municipality", "municipality_no IS NOT NULL"),  # count stations with a municipality ...
                       ("stations_matched_by_name", "municipality_match = 'name'")):  # ... and those matched by name
        metric("weather_station", name, stations.filter(cond).count())  # store the count
    metric("weather_station", "silver_rows", rows["weather_station"])  # rows written

    obs_in = read_bronze("frost_observations", split_on="referenceTime").persist(StorageLevel.MEMORY_AND_DISK)  # read observations by month, keep in memory
    temp_station, rej = clean_temperature(obs_in)               # clean them
    temp_station = in_period(temp_station, first_day, last_day)  # (Frost already downloads only this period)
    rejects.append(rej)                                         # collect the rejects
    temp_station = temp_station.persist()                       # keep it: used several times
    rows["temperature_station_daily"] = write_silver(temp_station, "temperature_station_daily")  # write temperature_station_daily
    metric("temperature_station_daily", "bronze_rows", obs_in.count())  # rows read from Bronze
    metric("temperature_station_daily", "rejected_rows", rej.count())  # rows rejected
    metric("temperature_station_daily", "silver_rows", rows["temperature_station_daily"])  # rows written
    metric("temperature_station_daily", "duplicate_keys", duplicate_keys(temp_station, "temperature_station_daily"))  # must be 0

    temp_muni = temperature_per_municipality(temp_station, stations, municipality)  # temperature per municipality
    rows["temperature_municipality_daily"] = write_silver(temp_muni, "temperature_municipality_daily")  # write it
    for method in ("stations", "county_average"):              # how many municipalities use which method
        n = temp_muni.filter(F.col("method") == method).select("municipality_no").distinct().count()  # municipalities using this method
        metric("temperature_municipality_daily", f"municipalities_{method}", n)  # store the count
    metric("temperature_municipality_daily", "municipalities_without_temperature",  # municipalities without any temperature ...
           municipality.join(temp_muni.select("municipality_no").distinct(), "municipality_no", "left_anti").count())  # ... must be 0
    metric("temperature_municipality_daily", "silver_rows", rows["temperature_municipality_daily"])  # rows written
    obs_in.unpersist(); temp_station.unpersist(); stations.unpersist()  # free the memory

    # --- 4.4 Norgespris (optional) -----------------------------------------------------------------
    if "norgespris_consumption_per_group_eac_mba" in available:  # optional dataset present?
        np_in = read_bronze("norgespris_consumption_per_group_eac_mba")  # read it
        norgespris, rej = clean_norgespris(np_in)               # clean it
        norgespris = in_period(norgespris, first_day, last_day)  # only days of the period
        rejects.append(rej)                                     # collect its rejects
        rows["norgespris_price_area_daily"] = write_silver(norgespris, "norgespris_price_area_daily")  # write norgespris_price_area_daily
        metric("norgespris_price_area_daily", "bronze_rows", np_in.count())  # rows read from Bronze
        metric("norgespris_price_area_daily", "rejected_rows", rej.count())  # rows rejected
        metric("norgespris_price_area_daily", "silver_rows", rows["norgespris_price_area_daily"])  # rows written
    else:                                                       # not in Bronze
        logger.info("  norgespris_consumption_per_group_eac_mba not in Bronze - skipped (optional)")  # skip it (optional)

    # --- 4.5 Norgespris meters per municipality (optional) ---------------------------------------------
    if "norgespris_count_per_municipality_consumption_group" in available:  # optional dataset present?
        npc_in = read_bronze("norgespris_count_per_municipality_consumption_group", split_on="DATE")  # read it by month
        npc_in = npc_in.persist(StorageLevel.MEMORY_AND_DISK)   # keep it: used several times
        norgespris_count, rej = clean_norgespris_count(npc_in, municipality)  # clean it
        norgespris_count = in_period(norgespris_count, first_day, last_day)  # only days of the period
        rejects.append(rej)                                     # collect its rejects
        rows["norgespris_count_daily"] = write_silver(norgespris_count, "norgespris_count_daily")  # write norgespris_count_daily
        metric("norgespris_count_daily", "bronze_rows", npc_in.count())  # rows read from Bronze
        metric("norgespris_count_daily", "rejected_rows", rej.count())  # rows rejected
        metric("norgespris_count_daily", "silver_rows", rows["norgespris_count_daily"])  # rows written
        metric("norgespris_count_daily", "duplicate_keys", duplicate_keys(norgespris_count, "norgespris_count_daily"))  # must be 0
        metric("norgespris_count_daily", "rows_unknown_municipality",  # rows of unknown municipalities ...
               norgespris_count.filter(~F.col("known_municipality")).count())  # ... counted
        npc_in.unpersist()                                      # free the memory
    else:                                                       # not in Bronze
        logger.info("  norgespris_count_per_municipality_consumption_group not in Bronze - skipped (optional)")  # skip it (optional)

    # --- 4.6 distribution key: private -> household + cabin -------------------------------------------
    def written(table):                                         # a Silver table read back (empty if it was skipped)
        if table in rows:                                       # the table was written in this run
            return spark.read.jdbc(PG_URL, f"silver.{table}", properties=JDBC_READ)  # read it back from PostgreSQL
        return spark.createDataFrame([], ", ".join(f"{n} {t}" for n, t in SILVER_SCHEMAS[table]))  # otherwise an empty table
    split = split_private(written("consumption_daily"), municipality, written("norgespris_count_daily"),  # build the distribution key ...
                          written("norgespris_price_area_daily")).persist(StorageLevel.MEMORY_AND_DISK)  # ... and keep it: used several times
    rows["private_split_daily"] = write_silver(split, "private_split_daily")  # write private_split_daily
    s = split.agg(F.sum("kwh_private").alias("private"),        # totals: private kWh ...
                  F.sum(F.col("kwh_household") + F.col("kwh_cabin")).alias("split"),  # ... household + cabin kWh ...
                  F.sum(F.when(F.col("key_method").isNull(), 1)).alias("no_key"),  # ... rows without a key ...
                  F.countDistinct(F.when(F.col("key_method").startswith("price_area"), F.col("municipality_no"))).alias("pa"),  # ... municipalities using the area fallback ...
                  F.sum(F.when(F.col("counted_meters_household").isNotNull(), F.col("metering_points_private"))).alias("mp"),  # ... private meters on days with counts ...
                  F.sum(F.coalesce("counted_meters_household", F.lit(0))  # ... meters counted by Elhub ...
                        + F.coalesce("counted_meters_cabin", F.lit(0))).alias("counted")).first()  # ... (household + cabin)
    metric("private_split_daily", "silver_rows", rows["private_split_daily"])  # rows written
    metric("private_split_daily", "duplicate_keys", duplicate_keys(split, "private_split_daily"))  # must be 0
    metric("private_split_daily", "kwh_not_split", float(s["private"] or 0) - float(s["split"] or 0))  # must be 0
    metric("private_split_daily", "rows_without_key", s["no_key"])  # rows without a key
    metric("private_split_daily", "municipalities_mix_from_price_area", s["pa"])  # municipalities using the area fallback
    metric("private_split_daily", "private_vs_counted_meters_pct",            # ~100 if private = household + cabin
           100 * float(s["mp"]) / float(s["counted"]) if s["mp"] and s["counted"] else 0)  # percent, 0 if no counts
    split.unpersist()                                           # free the memory
    municipality.unpersist()                                    # free the memory

    # --- rejects and metrics -------------------------------------------------------------------------
    all_rejects = rejects[0]                                    # start with the first rejects
    for r in rejects[1:]:                                       # add the others
        all_rejects = all_rejects.unionByName(r)                 # same three columns in every rejects DataFrame
    all_rejects = all_rejects.select(F.lit(run_id).alias("run_id"), "*")  # + the run id
    rows["rejects"] = write_silver(all_rejects, "rejects")      # write silver.rejects
    measured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")  # time of measuring (UTC)
    metrics_df = spark.createDataFrame([(r, measured_at, t, m, v) for r, t, m, v in metrics],  # metrics as a DataFrame ...
                                       "run_id string, measured_at string, table_name string, metric string, value double")  # ... with these columns
    metrics_df.write.jdbc(PG_URL, "silver.quality_metrics", mode="append", properties=PG_PROPERTIES)  # history

    elapsed = time.time() - start_time                          # run time in seconds
    failed_keys = [m for m in metrics if m[2] == "duplicate_keys" and m[3] > 0]  # grain broken -> stage fails
    status = "failed" if failed_keys else "success"             # duplicate keys -> failed stage
    logger.info(f"Silver done in {elapsed:.1f}s: {rows}" + (f" DUPLICATE KEYS: {failed_keys}" if failed_keys else ""))  # log the result
    return {"status": status, "elapsed_time": elapsed, "run_id": run_id, "rows": rows}  # summary for run_pipeline.py


if __name__ == "__main__":                                      # only when run as a script
    result = run_silver()                                      # build all Silver tables
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
