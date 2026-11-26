#!/usr/bin/env python3
"""
DAT535 Group 24 - Bronze layer: landing files -> PostgreSQL (schema bronze)
==========================================================================

GENERATED from the code cells marked for PostgreSQL in notebooks/01_bronze.ipynb
(4.1 settings, 4.2/4.5 readers, 5.1-5.2 loader). The notebook explains every line;
this file is what GitHub Actions runs. Settings and password: .env / environment
(NORGESPRIS_PG_HOST, _PORT, _USER, _PASSWORD, _DATABASE, _JDBC_JAR).

Delta load (NORGESPRIS_LOAD_MODE=delta): time-series tables only get the landing
files that are not in them yet (appended); bronze._load_files records which files
each table holds. Other tables, and the first load, replace the whole table.

Usage:
    python run_pipeline.py postgres    (recommended; after "bronze" has filled the landing folder)
    python bronze_to_postgres.py
"""

import logging                                                  # log messages
import os                                                       # environment variables and paths

# Same settings and source registry as the landing ingestion (this also reads .env):
from bronze_pipeline import ENV, LANDING_DIR, MANIFEST_PATH, SOURCES, LOAD_MODE  # noqa: F401

spark = None                                                    # set by run_bronze_to_postgres()
manifest = None                                                 # set by run_bronze_to_postgres()


from pyspark.sql import SparkSession                            # entry point to Spark
from pyspark.sql import functions as F                          # column functions (count, min, max, ...)
from pyspark.sql import types as T                              # data types, to find nested JSON columns

PG_HOST = os.environ.get("NORGESPRIS_PG_HOST", "localhost")     # machine where PostgreSQL runs
PG_PORT = os.environ.get("NORGESPRIS_PG_PORT", "5432")          # PostgreSQL's standard port
PG_USER = os.environ.get("NORGESPRIS_PG_USER", "norgespris")    # the role created with scripts/init_postgres.sql
PG_PASSWORD = os.environ.get("NORGESPRIS_PG_PASSWORD", "")      # from .env or a GitHub secret - never in the code
PG_DATABASE = os.environ.get("NORGESPRIS_PG_DATABASE") or f"norgespris_{ENV}"  # norgespris_dev / norgespris_prod
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}" # JDBC address Spark connects to (no password in it)
PG_PROPERTIES = {"user": PG_USER, "password": PG_PASSWORD,      # login for every JDBC read/write
                 "driver": "org.postgresql.Driver"}             # Java class of the PostgreSQL driver
PG_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"            # Maven coordinates of the driver
PG_JDBC_JAR = os.environ.get("NORGESPRIS_PG_JDBC_JAR", "").strip()  # optional: local .jar instead of Maven
SPARK_DRIVER_MEMORY = os.environ.get("NORGESPRIS_SPARK_DRIVER_MEMORY", "2g")  # lower (e.g. 1g) on a machine with little RAM


def get_spark():                                                # one SparkSession for all stages
    """Start (or reuse) the SparkSession, with the PostgreSQL JDBC driver on the classpath."""
    builder = (SparkSession.builder                             # start the settings of the session
               .appName("DAT535-Group24-Bronze")                # name in the Spark UI
               .config("spark.sql.adaptive.enabled", "true")    # let Spark tune partitions/joins at runtime
               .config("spark.sql.shuffle.partitions", "8")     # small data -> few shuffle partitions
               .config("spark.driver.memory", SPARK_DRIVER_MEMORY)  # Java heap for Spark (local mode: driver = executor)
               .config("spark.sql.debug.maxToStringFields", "200"))  # wide tables: no "Truncated the string representation" warning
    if PG_JDBC_JAR:                                             # a downloaded driver file ...
        builder = builder.config("spark.jars", PG_JDBC_JAR)     # use the local .jar file
    else:                                                       # ... or let Spark fetch it from Maven Central
        builder = builder.config("spark.jars.packages", PG_JDBC_PACKAGE)  # Spark downloads the driver once
    session = builder.getOrCreate()                             # reuse a running session if there is one
    session.sparkContext.setLogLevel("WARN")                    # hide Spark's INFO noise
    return session                                              # the ready SparkSession


def newest_files(dataset):                                      # which landing files to read for a dataset
    """Absolute paths of the newest ingested version of every file of a dataset."""
    all_rows = manifest[manifest.dataset == dataset]            # manifest rows of this dataset
    if dataset == "frost_observations" and not all_rows.empty:  # Frost: only the newest station grouping
        all_rows = all_rows.copy()                              # copy, so the manifest itself is not changed
        all_rows["_layout"] = all_rows.original_filename.str.extract(r"_l([0-9a-f]{12})_g\d+\.json$")[0].fillna("legacy")  # grouping id from the file name
        layouts = all_rows[all_rows.status.isin(("ingested", "skipped_duplicate", "no_data"))]  # rows of successful or empty requests
        latest_layout = layouts.sort_values("run_id").groupby("period").tail(1)[["period", "_layout"]]  # newest grouping per month
        all_rows = all_rows.merge(latest_layout, on=["period", "_layout"])  # keep only files of that grouping
    rows = all_rows[all_rows.status.isin(("ingested", "skipped_duplicate"))  # only stored files ...
                    & all_rows.landing_path.notna()]  # latest successful fetches with a stored path
    newest = rows.sort_values("run_id").groupby(["period", "original_filename"]).tail(1)  # newest per file
    paths = [os.path.join(LANDING_DIR, p) for p in newest.landing_path]                 # full paths
    return [p[:-len(".zip")] + "_unzipped" if p.endswith(".zip") else p for p in paths]  # ZIP -> its unpacked folder

def guess_sep(dataset, default):                                # CSV separator of a dataset
    """Separator from the header line stored in the manifest: ';' if it has more ';' than ','."""
    rows = manifest[(manifest.dataset == dataset) & manifest.status.isin(("ingested", "skipped_duplicate"))]  # stored files of this dataset
    if rows.empty:                                              # nothing stored -> use the registry value
        return default                                          # registry value
    header = rows.sort_values("run_id").header.iloc[-1]         # header of the newest file
    return ";" if header.count(";") > header.count(",") else ","  # the more frequent character wins

def guess_encoding(path):                                       # text encoding of a CSV file
    """'UTF-8' if the start of the file is valid UTF-8, otherwise 'ISO-8859-1' (Latin-1, e.g. SSB's API: 'å' = one byte)."""
    if os.path.isdir(path):                                     # unpacked ZIP folder -> look at its first CSV
        csvs = sorted(f for f in os.listdir(path) if f.lower().endswith(".csv"))  # CSV files in the folder
        if not csvs:                                            # no CSV in the folder
            return "UTF-8"                                      # default
        path = os.path.join(path, csvs[0])                      # the first CSV decides
    with open(path, "rb") as f:                                 # read raw bytes
        sample = f.read(65536)                                  # the first 64 KB are enough to decide
    try:                                                        # try to decode as UTF-8
        sample.decode("utf-8")                                  # valid UTF-8 (plain ASCII is too) -> UTF-8
        return "UTF-8"                                          # it worked
    except UnicodeDecodeError as e:                             # not valid UTF-8 somewhere
        if e.start >= len(sample) - 3:                          # only the last character was cut by the 64 KB limit
            return "UTF-8"                                      # the file is still UTF-8
        return "ISO-8859-1"                                     # bytes that are not UTF-8 -> Latin-1 (Norwegian letters = 1 byte)

def read_bronze(cfg, files=None):                               # read a CSV dataset from landing
    """Read a Bronze CSV dataset with Spark, all columns as text; None if nothing to read.
    files: read only these (delta load); default = the newest version of every file of the dataset."""
    files = newest_files(cfg["dataset"]) if files is None else files  # which files/folders to read
    if not files or cfg["sep"] is None:                         # nothing ingested, or JSON (Frost -> 4.5)
        return None                                             # nothing to read here
    sep = guess_sep(cfg["dataset"], cfg["sep"])                 # Elhub's ZIP may use another separator than our samples
    by_encoding = {}                                            # {'UTF-8': [files], 'ISO-8859-1': [files]}
    for path in files:                                          # group the files by encoding
        by_encoding.setdefault(guess_encoding(path), []).append(path)  # add the file to its encoding group
    parts = []                                                  # one DataFrame per encoding
    for encoding, group in by_encoding.items():                 # one Spark read per encoding
        df = (spark.read                                        # Spark CSV reader with options
              .option("header", True)                           # first line = column names
              .option("inferSchema", False)                     # keep everything as text (raw)
              .option("sep", sep)                               # separator for this dataset
              .option("encoding", encoding)                     # the encoding the files are written in
              .csv(group))                                      # list of files -> one DataFrame
        clean = [c.replace("\ufeff", "").strip() for c in df.columns]  # remove BOM from column names only
        parts.append(df.select(*[F.col(f"`{c}`").alias(n) for c, n in zip(df.columns, clean)],  # same columns, clean names
                               F.col("_metadata.file_path").alias("_source_file")))         # + which landing file
    result = parts[0]                                           # start with the first part
    for part in parts[1:]:                                      # files with different encodings -> one DataFrame
        result = result.unionByName(part, allowMissingColumns=True)  # columns only in some files -> NULL elsewhere
    return result                                               # one DataFrame with all files


def read_frost(dataset, files=None):                            # read a Frost JSON dataset from landing
    """Read all newest Frost JSON files of a dataset (or only `files`) with Spark and explode 'data' into rows."""
    files = newest_files(dataset) if files is None else files   # all JSON files of the dataset (or the given ones)
    if not files:                                               # no files stored
        print(f"-- {dataset}: nothing in landing yet (is FROST_CLIENT_ID set?)")  # say why
        return None                                             # nothing to read
    raw = spark.read.option("multiLine", True).json(files)      # one JSON document per file
    return (raw.select(F.explode("data").alias("d"),            # one row per element of "data"
                       F.col("_metadata.file_path").alias("_source_file"))  # + which landing file
               .select("d.*", "_source_file"))                  # the fields of each record + file


import hashlib                                                  # fingerprint of the landing files of a dataset
import time                                                     # elapsed time
from datetime import datetime, timezone                         # load timestamps (UTC)

import pandas as pd                                             # the manifest is a small table

pg_logger = logging.getLogger("postgres")                       # own logger name in the run log


def to_postgres_types(df):                                      # make every column storable as text
    """Nested columns (struct/array/map, e.g. Frost's lists) -> JSON text; other columns unchanged."""
    cols = []                                                   # the new column list
    for field in df.schema.fields:                              # look at every column's data type
        name = f"`{field.name}`"                                # backticks: names may contain spaces/dots
        if isinstance(field.dataType, (T.StructType, T.ArrayType, T.MapType)):  # nested column (list or object)?
            cols.append(F.to_json(F.col(name)).alias(field.name))  # nested -> JSON text
        else:                                                   # simple column
            cols.append(F.col(name).cast("string").alias(field.name))  # everything else as text
    return df.select(*cols)                                     # the converted DataFrame


def add_lineage(df):                                            # add the lineage columns
    """Add _landing_run_id (from the file name '<run_id>__...') and _loaded_at to every row."""
    return (df.withColumn("_landing_run_id",                    # run id from the file name ...
                          F.regexp_extract("_source_file", r"/(\d{8}T\d+Z)__", 1))  # run that stored the file
              .withColumn("_loaded_at", F.current_timestamp()))             # when it was written to Postgres


def bronze_fingerprint(dataset):                                # short id of the landing files of a dataset
    """Short hash of the newest landing file paths of a dataset (paths contain the run_id) - None if no files."""
    files = newest_files(dataset)                               # same file selection as the profiling
    if not files:                                               # no files
        return None                                             # nothing to load
    return hashlib.sha256("\n".join(sorted(files)).encode("utf-8")).hexdigest()[:16]  # 16 characters of the SHA-256


def pg_query(sql):                                              # SELECT inside PostgreSQL
    """Run a SELECT in PostgreSQL through Spark and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=PG_PROPERTIES)  # the query runs inside Postgres


def write_table(df, table, mode="overwrite"):                   # write a DataFrame to PostgreSQL
    """Write a DataFrame to a PostgreSQL table (overwrite = drop and recreate, append = add rows)."""
    (df.write                                                   # write ...
       .option("batchsize", 10000)                              # rows per INSERT batch - much faster than 1 by 1
       .jdbc(PG_URL, table, mode=mode, properties=PG_PROPERTIES))  # ... over JDBC to the table


def read_load_log():                                            # what was loaded last time
    """Last fingerprint per dataset from bronze._load_log ({} on the very first load)."""
    try:                                                        # the log table may not exist yet
        log = pg_query("SELECT dataset, fingerprint, loaded_at FROM bronze._load_log").toPandas()  # read the log into pandas (small)
    except Exception as e:                                      # the table does not exist before the first load
        if "does not exist" in str(e):                          # first load ever
            return {}                                           # no fingerprints yet
        raise                                                   # other errors (wrong password, no server) -> stop
    newest = log.sort_values("loaded_at").groupby("dataset").tail(1)   # newest load per dataset
    return dict(zip(newest.dataset, newest.fingerprint))        # dataset -> fingerprint


def has_delta(cfg):                                             # is this dataset loaded month by month in delta mode?
    """True for the time series that landing stores as monthly files (Elhub with delta_url, Frost observations)."""
    return bool(cfg.get("delta_url")) or cfg["mode"] == "frost_observations"  # the rest are small full files


def files_since_full_load(log):                                 # which landing files are in each bronze table now
    """From the rows of bronze._load_files: per dataset the files of its newest full load + every append after it."""
    loaded = {}                                                 # dataset -> set of landing file paths
    for dataset, rows in log.groupby("dataset"):                # one dataset at a time
        full = rows[rows.load_type == "full"].loaded_at         # times of its full loads
        since = full.max() if not full.empty else ""            # newest full load ('' = none -> every row counts)
        loaded[dataset] = set(rows[rows.loaded_at >= since].landing_file)  # files written since then
    return loaded                                               # e.g. {'frost_observations': {'/tmp/...json', ...}}


def read_loaded_files():                                        # what is already in the bronze tables
    """Files already loaded into each bronze table, from bronze._load_files ({} before the first load)."""
    try:                                                        # the table may not exist yet
        log = pg_query("SELECT dataset, landing_file, load_type, loaded_at FROM bronze._load_files").toPandas()  # small table
    except Exception as e:                                      # the table does not exist before the first load
        if "does not exist" in str(e):                          # first load with delta support
            return {}                                           # nothing known -> full loads
        raise                                                   # other errors (wrong password, no server) -> stop
    return files_since_full_load(log)                           # dataset -> files


def is_month_file(path):                                        # a delta file (stored under a month folder)?
    """True for landing files under .../<dataset>/2026-10/..., False for full files (.../snapshot/...)."""
    period = os.path.basename(os.path.dirname(path))            # the folder above the file = its period
    return len(period) == 7 and period[4] == "-"                # 'YYYY-MM' = one month


def table_columns(table):                                       # column names of a table in PostgreSQL
    """Column names of an existing table, None if the table does not exist."""
    try:                                                        # LIMIT 0 = only the column names, no rows
        return pg_query(f"SELECT * FROM {table} LIMIT 0").columns  # Spark reads the schema of the result
    except Exception as e:                                      # e.g. the table was dropped by hand
        if "does not exist" in str(e):                          # table missing
            return None                                         # -> the caller does a full load
        raise                                                   # other errors -> stop


def load_dataset(cfg, last_fingerprints, loaded_files=None):    # load one dataset into bronze
    """Load one dataset from landing into bronze.<dataset>; returns a dict for the load logs.
    Delta mode: only landing files that are not in the table yet are appended (loaded_files = what is in it)."""
    dataset = cfg["dataset"]                                    # dataset name
    table = f"bronze.{dataset}"                                 # target table
    result = {"dataset": dataset, "table": table, "status": "", "fingerprint": "", "rows": 0,  # result row for the load log ...
              "files": [], "load_type": ""}                     # ... + which files were written (for bronze._load_files)
    fingerprint = bronze_fingerprint(dataset)                   # which landing files would be loaded
    if fingerprint is None:                                     # nothing in landing (e.g. optional source)
        result["status"] = "no_landing_files"                   # nothing in landing
        return result                                           # skip this dataset
    result["fingerprint"] = fingerprint                         # remember which files
    if last_fingerprints.get(dataset) == fingerprint:           # same files as last time
        result["status"] = "unchanged"                          # nothing changed
        return result                                           # skip: no need to reload

    def read(files):                                            # CSV or JSON reader from section 4
        return read_bronze(cfg, files) if cfg["sep"] is not None else read_frost(dataset, files)  # only these files

    files = newest_files(dataset)                               # every newest landing file of the dataset
    done = (loaded_files or {}).get(dataset)                    # files already in the table (None = not known)
    if LOAD_MODE == "delta" and has_delta(cfg) and done:        # delta: try to append only the new files
        new = [f for f in files if f not in done]               # e.g. the file of last month
        only_months = bool(new) and all(is_month_file(f) for f in new)  # a new FULL file must replace the table, not be added
        columns = table_columns(table) if only_months else None  # columns of the existing table
        if columns is not None:                                 # there are new files and the table exists
            df = add_lineage(to_postgres_types(read(new)))      # text columns + lineage, new files only
            if set(df.columns) == set(columns):                 # same columns -> appending is safe
                result["rows"] = df.count()                     # rows in the new files
                write_table(df.select(*columns), table, mode="append")  # add them (same column order as the table)
                result.update(status="appended", files=new, load_type="delta")  # remember what was added
                pg_logger.info(f"  {table}: +{result['rows']:,} rows from {len(new)} new file(s)")  # log the delta
                return result                                   # done: older rows stay as they are
        pg_logger.info(f"  {table}: cannot append (a new full file, no table or new columns) -> full reload")  # explain

    df = read(files)                                            # CSV or JSON reader from section 4
    df = add_lineage(to_postgres_types(df))                     # text columns + lineage
    write_table(df, table)                                      # replace the table
    result["rows"] = pg_query(f"SELECT COUNT(*) AS n FROM {table}").first()["n"]  # rows that arrived in Postgres
    result.update(status="loaded", files=files, load_type="full")  # loaded (all newest files)
    pg_logger.info(f"  {table}: {result['rows']:,} rows")       # log the row count
    return result                                               # result for the load log


def run_bronze_to_postgres():                                   # the whole postgres stage
    """Load the newest landing files and the manifest into the bronze schema in PostgreSQL; returns a summary."""
    global spark, manifest                                      # used by the reader functions from section 4
    start_time = time.time()                                    # start time
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):      # missing or still the placeholder from env.example
        pg_logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")  # clear message, no stack trace
        return {"status": "failed", "elapsed_time": 0.0}        # stop without starting Spark
    spark = get_spark()                                         # start Spark with the JDBC driver
    manifest = pd.read_csv(MANIFEST_PATH, dtype=str).fillna("") # newest manifest (landing may just have run)
    pg_logger.info(f"Loading landing files into the bronze schema of {PG_URL}")  # log where we write
    last = read_load_log()                                      # what was loaded last time
    loaded_files = read_loaded_files()                          # which landing files are in the tables (for delta)
    pg_logger.info(f"Load mode: {LOAD_MODE}")                   # delta = append new files, full = replace tables
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")  # one timestamp for this load
    results, failed = [], 0                                     # results and number of failures
    for cfg in SOURCES:                                         # same order as the registry
        try:                                                    # one failing dataset must not stop the others
            results.append(load_dataset(cfg, last, loaded_files))  # load and remember the result
        except Exception as e:                                  # keep going, but remember the failure
            pg_logger.error(f"  bronze.{cfg['dataset']}: {type(e).__name__}: {str(e)[:300]}")  # log the error (shortened)
            results.append({"dataset": cfg["dataset"], "table": f"bronze.{cfg['dataset']}",  # result row for the failure ...
                            "status": f"failed: {type(e).__name__}", "fingerprint": "", "rows": 0})  # ... with the error type
            failed += 1 if cfg["required"] else 0               # optional datasets do not fail the stage

    write_table(spark.createDataFrame(manifest), "bronze.manifest")  # the manifest as a table (replaced each run)
    log_cols = ("dataset", "table", "status", "fingerprint", "rows")  # the columns of bronze._load_log
    done = [dict({c: r[c] for c in log_cols}, loaded_at=loaded_at)  # one log row per dataset ...
            for r in results if r["status"] in ("loaded", "appended")]  # ... that was loaded or appended
    if done:                                                    # remember what was loaded (append = history)
        write_table(spark.createDataFrame(pd.DataFrame(done)), "bronze._load_log", mode="append")  # append to the load history
    files = [{"dataset": r["dataset"], "landing_file": f, "load_type": r["load_type"], "loaded_at": loaded_at}  # one row ...
             for r in results if r.get("files") for f in r["files"]]  # ... per landing file written this run
    if files:                                                   # remember which files are in the tables (for delta)
        write_table(spark.createDataFrame(pd.DataFrame(files)), "bronze._load_files", mode="append")  # append = history

    elapsed = time.time() - start_time                          # run time in seconds
    counts = pd.Series([r["status"].split(":")[0] for r in results]).value_counts().to_dict()  # count the results by status
    pg_logger.info(f"PostgreSQL load done in {elapsed:.1f}s: {counts}")  # log the totals
    return {"status": "success" if failed == 0 else "failed", "elapsed_time": elapsed,  # summary for run_pipeline.py ...
            "tables": results, **counts}                        # ... with the per-table results


if __name__ == "__main__":                                      # only when run as a script
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # log format when run alone
    result = run_bronze_to_postgres()                          # load all datasets
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
