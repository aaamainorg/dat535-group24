#!/usr/bin/env python3
"""
DAT535 Group 24 - Bronze layer: landing files -> PostgreSQL (schema bronze)
==========================================================================

GENERATED from the code cells marked for PostgreSQL in notebooks/01_bronze.ipynb
(4.1 settings, 4.2/4.5 readers, 5.1-5.2 loader). The notebook explains every line;
this file is what GitHub Actions runs. Settings and password: .env / environment
(NORGESPRIS_PG_HOST, _PORT, _USER, _PASSWORD, _DATABASE, _JDBC_JAR).

Usage:
    python run_pipeline.py postgres    (recommended; after "bronze" has filled the landing folder)
    python bronze_to_postgres.py
"""

import logging                                                  # log messages
import os                                                       # environment variables and paths

# Same settings and source registry as the landing ingestion (this also reads .env):
from bronze_pipeline import ENV, LANDING_DIR, MANIFEST_PATH, SOURCES  # noqa: F401

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


def get_spark():
    """Start (or reuse) the SparkSession, with the PostgreSQL JDBC driver on the classpath."""
    builder = (SparkSession.builder
               .appName("DAT535-Group24-Bronze")                # name in the Spark UI
               .config("spark.sql.adaptive.enabled", "true")    # let Spark tune partitions/joins at runtime
               .config("spark.sql.shuffle.partitions", "8")     # small data -> few shuffle partitions
               .config("spark.driver.memory", "2g")             # enough for this data on a laptop
               .config("spark.sql.debug.maxToStringFields", "200"))  # wide tables: no "Truncated the string representation" warning
    if PG_JDBC_JAR:                                             # a downloaded driver file ...
        builder = builder.config("spark.jars", PG_JDBC_JAR)
    else:                                                       # ... or let Spark fetch it from Maven Central
        builder = builder.config("spark.jars.packages", PG_JDBC_PACKAGE)
    session = builder.getOrCreate()                             # reuse a running session if there is one
    session.sparkContext.setLogLevel("WARN")                    # hide Spark's INFO noise
    return session


def newest_files(dataset):
    """Absolute paths of the newest ingested version of every file of a dataset."""
    all_rows = manifest[manifest.dataset == dataset]
    if dataset == "frost_observations" and not all_rows.empty:
        all_rows = all_rows.copy()
        all_rows["_layout"] = all_rows.original_filename.str.extract(r"_l([0-9a-f]{12})_g\d+\.json$")[0].fillna("legacy")
        layouts = all_rows[all_rows.status.isin(("ingested", "skipped_duplicate", "no_data"))]
        latest_layout = layouts.sort_values("run_id").groupby("period").tail(1)[["period", "_layout"]]
        all_rows = all_rows.merge(latest_layout, on=["period", "_layout"])
    rows = all_rows[all_rows.status.isin(("ingested", "skipped_duplicate"))
                    & all_rows.landing_path.notna()]  # latest successful fetches with a stored path
    newest = rows.sort_values("run_id").groupby(["period", "original_filename"]).tail(1)  # newest per file
    paths = [os.path.join(LANDING_DIR, p) for p in newest.landing_path]                 # full paths
    return [p[:-len(".zip")] + "_unzipped" if p.endswith(".zip") else p for p in paths]  # ZIP -> its unpacked folder

def guess_sep(dataset, default):
    """Separator from the header line stored in the manifest: ';' if it has more ';' than ','."""
    rows = manifest[(manifest.dataset == dataset) & manifest.status.isin(("ingested", "skipped_duplicate"))]
    if rows.empty:                                              # nothing stored -> use the registry value
        return default
    header = rows.sort_values("run_id").header.iloc[-1]         # header of the newest file
    return ";" if header.count(";") > header.count(",") else ","

def guess_encoding(path):
    """'UTF-8' if the start of the file is valid UTF-8, otherwise 'ISO-8859-1' (Latin-1, e.g. SSB's API: 'å' = one byte)."""
    if os.path.isdir(path):                                     # unpacked ZIP folder -> look at its first CSV
        csvs = sorted(f for f in os.listdir(path) if f.lower().endswith(".csv"))
        if not csvs:
            return "UTF-8"
        path = os.path.join(path, csvs[0])
    with open(path, "rb") as f:
        sample = f.read(65536)                                  # the first 64 KB are enough to decide
    try:
        sample.decode("utf-8")                                  # valid UTF-8 (plain ASCII is too) -> UTF-8
        return "UTF-8"
    except UnicodeDecodeError as e:
        if e.start >= len(sample) - 3:                          # only the last character was cut by the 64 KB limit
            return "UTF-8"
        return "ISO-8859-1"                                     # bytes that are not UTF-8 -> Latin-1 (Norwegian letters = 1 byte)

def read_bronze(cfg):
    """Read a Bronze CSV dataset with Spark, all columns as text; None if nothing to read."""
    files = newest_files(cfg["dataset"])                        # which files/folders to read
    if not files or cfg["sep"] is None:                         # nothing ingested, or JSON (Frost -> 4.5)
        return None
    sep = guess_sep(cfg["dataset"], cfg["sep"])                 # Elhub's ZIP may use another separator than our samples
    by_encoding = {}                                            # {'UTF-8': [files], 'ISO-8859-1': [files]}
    for path in files:
        by_encoding.setdefault(guess_encoding(path), []).append(path)
    parts = []
    for encoding, group in by_encoding.items():                 # one Spark read per encoding
        df = (spark.read
              .option("header", True)                           # first line = column names
              .option("inferSchema", False)                     # keep everything as text (raw)
              .option("sep", sep)                               # separator for this dataset
              .option("encoding", encoding)                     # the encoding the files are written in
              .csv(group))                                      # list of files -> one DataFrame
        clean = [c.replace("\ufeff", "").strip() for c in df.columns]  # remove BOM from column names only
        parts.append(df.select(*[F.col(f"`{c}`").alias(n) for c, n in zip(df.columns, clean)],  # same columns, clean names
                               F.col("_metadata.file_path").alias("_source_file")))         # + which landing file
    result = parts[0]
    for part in parts[1:]:                                      # files with different encodings -> one DataFrame
        result = result.unionByName(part, allowMissingColumns=True)  # columns only in some files -> NULL elsewhere
    return result


def read_frost(dataset):
    """Read all newest Frost JSON files of a dataset with Spark and explode the 'data' list into rows."""
    files = newest_files(dataset)                               # all JSON files of the dataset
    if not files:
        print(f"-- {dataset}: nothing in landing yet (is FROST_CLIENT_ID set?)")
        return None
    raw = spark.read.option("multiLine", True).json(files)      # one JSON document per file
    return (raw.select(F.explode("data").alias("d"),            # one row per element of "data"
                       F.col("_metadata.file_path").alias("_source_file"))  # + which landing file
               .select("d.*", "_source_file"))


import hashlib                                                  # fingerprint of the landing files of a dataset
import time                                                     # elapsed time
from datetime import datetime, timezone                         # load timestamps (UTC)

import pandas as pd                                             # the manifest is a small table

pg_logger = logging.getLogger("postgres")                       # own logger name in the run log


def to_postgres_types(df):
    """Nested columns (struct/array/map, e.g. Frost's lists) -> JSON text; other columns unchanged."""
    cols = []
    for field in df.schema.fields:                              # look at every column's data type
        name = f"`{field.name}`"                                # backticks: names may contain spaces/dots
        if isinstance(field.dataType, (T.StructType, T.ArrayType, T.MapType)):
            cols.append(F.to_json(F.col(name)).alias(field.name))  # nested -> JSON text
        else:
            cols.append(F.col(name).cast("string").alias(field.name))  # everything else as text
    return df.select(*cols)


def add_lineage(df):
    """Add _landing_run_id (from the file name '<run_id>__...') and _loaded_at to every row."""
    return (df.withColumn("_landing_run_id",
                          F.regexp_extract("_source_file", r"/(\d{8}T\d+Z)__", 1))  # run that stored the file
              .withColumn("_loaded_at", F.current_timestamp()))             # when it was written to Postgres


def bronze_fingerprint(dataset):
    """Short hash of the newest landing file paths of a dataset (paths contain the run_id) - None if no files."""
    files = newest_files(dataset)                               # same file selection as the profiling
    if not files:
        return None
    return hashlib.sha256("\n".join(sorted(files)).encode("utf-8")).hexdigest()[:16]


def pg_query(sql):
    """Run a SELECT in PostgreSQL through Spark and return the result as a Spark DataFrame."""
    return spark.read.jdbc(PG_URL, f"({sql}) AS q", properties=PG_PROPERTIES)  # the query runs inside Postgres


def write_table(df, table, mode="overwrite"):
    """Write a DataFrame to a PostgreSQL table (overwrite = drop and recreate, append = add rows)."""
    (df.write
       .option("batchsize", 10000)                              # rows per INSERT batch - much faster than 1 by 1
       .jdbc(PG_URL, table, mode=mode, properties=PG_PROPERTIES))


def read_load_log():
    """Last fingerprint per dataset from bronze._load_log ({} on the very first load)."""
    try:
        log = pg_query("SELECT dataset, fingerprint, loaded_at FROM bronze._load_log").toPandas()
    except Exception as e:                                      # the table does not exist before the first load
        if "does not exist" in str(e):
            return {}
        raise                                                   # other errors (wrong password, no server) -> stop
    newest = log.sort_values("loaded_at").groupby("dataset").tail(1)   # newest load per dataset
    return dict(zip(newest.dataset, newest.fingerprint))


def load_dataset(cfg, last_fingerprints):
    """Load one dataset from landing into bronze.<dataset>; returns a dict for the load log."""
    dataset = cfg["dataset"]
    table = f"bronze.{dataset}"
    result = {"dataset": dataset, "table": table, "status": "", "fingerprint": "", "rows": 0}
    fingerprint = bronze_fingerprint(dataset)                   # which landing files would be loaded
    if fingerprint is None:                                     # nothing in landing (e.g. optional source)
        result["status"] = "no_landing_files"
        return result
    result["fingerprint"] = fingerprint
    if last_fingerprints.get(dataset) == fingerprint:           # same files as last time
        result["status"] = "unchanged"
        return result
    df = read_bronze(cfg) if cfg["sep"] is not None else read_frost(dataset)  # CSV or JSON reader from section 4
    df = add_lineage(to_postgres_types(df))                     # text columns + lineage
    write_table(df, table)                                      # replace the table
    result["rows"] = pg_query(f"SELECT COUNT(*) AS n FROM {table}").first()["n"]  # rows that arrived in Postgres
    result["status"] = "loaded"
    pg_logger.info(f"  {table}: {result['rows']:,} rows")
    return result


def run_bronze_to_postgres():
    """Load the newest landing files and the manifest into the bronze schema in PostgreSQL; returns a summary."""
    global spark, manifest                                      # used by the reader functions from section 4
    start_time = time.time()
    if not PG_PASSWORD or PG_PASSWORD.startswith("your-"):      # missing or still the placeholder from env.example
        pg_logger.error("NORGESPRIS_PG_PASSWORD is not set (in .env or as GitHub secret)")  # clear message, no stack trace
        return {"status": "failed", "elapsed_time": 0.0}
    spark = get_spark()
    manifest = pd.read_csv(MANIFEST_PATH, dtype=str).fillna("") # newest manifest (landing may just have run)
    pg_logger.info(f"Loading landing files into the bronze schema of {PG_URL}")
    last = read_load_log()                                      # what was loaded last time
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    results, failed = [], 0
    for cfg in SOURCES:                                         # same order as the registry
        try:
            results.append(load_dataset(cfg, last))
        except Exception as e:                                  # keep going, but remember the failure
            pg_logger.error(f"  bronze.{cfg['dataset']}: {type(e).__name__}: {str(e)[:300]}")
            results.append({"dataset": cfg["dataset"], "table": f"bronze.{cfg['dataset']}",
                            "status": f"failed: {type(e).__name__}", "fingerprint": "", "rows": 0})
            failed += 1 if cfg["required"] else 0               # optional datasets do not fail the stage

    write_table(spark.createDataFrame(manifest), "bronze.manifest")  # the manifest as a table (replaced each run)
    done = [dict(r, loaded_at=loaded_at) for r in results if r["status"] == "loaded"]
    if done:                                                    # remember what was loaded (append = history)
        write_table(spark.createDataFrame(pd.DataFrame(done)), "bronze._load_log", mode="append")

    elapsed = time.time() - start_time
    counts = pd.Series([r["status"].split(":")[0] for r in results]).value_counts().to_dict()
    pg_logger.info(f"PostgreSQL load done in {elapsed:.1f}s: {counts}")
    return {"status": "success" if failed == 0 else "failed", "elapsed_time": elapsed,
            "tables": results, **counts}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    result = run_bronze_to_postgres()                          # load all datasets
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
