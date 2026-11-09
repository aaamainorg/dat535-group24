#!/usr/bin/env python3
"""
DAT535 Group 24 - Bronze layer (ingest and preserve)
=====================================================

GENERATED from the code cells in sections 3.1-3.9 of
notebooks/01_scope_dataset_bronze.ipynb. The notebook explains every line;
this file is what GitHub Actions runs. If you change the logic, change the
notebook cell and copy it here, so the two stay identical.

Usage:
    python run_pipeline.py bronze      (recommended)
    python bronze_pipeline.py
"""

import base64                                  # build the HTTP Basic-auth header for the Frost API
import csv                                     # read and write the manifest file (handles commas/quotes in text)
import gzip                                    # data.elhub.no sends files gzip-compressed
import hashlib                                 # SHA-256 checksums to recognise identical files
import logging                                 # log messages that also show up in GitHub Actions
import os                                      # environment variables and file paths
import shutil                                  # copy files and delete temp folders
import time                                    # measure run time and wait between download retries
import urllib.request                          # download files over HTTPS (standard library, no pip install needed)
import zipfile                                 # Elhub's ".csv" exports are really ZIP files with a CSV inside
from datetime import datetime, timezone, date  # timestamps (always UTC) and month calculations

logging.basicConfig(level=logging.INFO,                                        # show INFO and above
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # time - logger - level - text
logger = logging.getLogger("bronze")                                          # named logger for this layer


ENV = os.environ.get("DAT535_ENV", "dev")                     # "dev" or "prod"; default dev when run by hand
DATA_ROOT = os.environ.get("DAT535_OUTPUT_DIR",               # root folder chosen by the workflow ...
                           os.path.expanduser(f"~/dat535-data/{ENV}"))  # ... or ~/dat535-data/dev locally

LANDING_DIR = os.path.join(DATA_ROOT, "landing")              # put manually exported files here
BRONZE_DIR = os.path.join(DATA_ROOT, "bronze")                # raw files, never edited
MANIFEST_PATH = os.path.join(BRONZE_DIR, "_manifest.csv")     # one line per file we have seen

MONTH_RANGE = {                                               # which months to fetch from the Elhub API
    "dev": ("2025-09", "2025-10"),                            # dev: 2 months (only used by "monthly" sources)
    "prod": ("2023-11", None),                                # prod: Nov 2023 -> last complete month (None)
}
REDOWNLOAD_RECENT_MONTHS = 2                                  # always re-fetch the newest months (late/revised data)


ELHUB_API = "https://api.elhub.no/energy-data/v0"             # Elhub Energy Data API (open, no login)

SSB_URL = ("https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"      # SSB table 07459, Norwegian labels
           "&valueCodes[Region]=*&codelist[Region]=agg_KommSummer"             # all municipalities, today's structure
           "&outputValues[Region]=aggregated"                                  # sums for merged municipalities
           "&valueCodes[ContentsCode]=*&valueCodes[Tid]=from(2023)"            # population, years 2023 ->
           "&outputFormat=csv&outputFormatParams=separatorsemicolon")          # CSV with ';' like the manual export
# Sex and age are left out of the query -> SSB sums them ("elimination"), so we get total population.

SOURCES = [                                                                    # the datasets the pipeline handles
    {"source": "elhub", "dataset": "consumption_per_group_municipality_hour",  # hourly kWh per municipality and group
     "mode": "snapshot", "sep": ";", "required": True,                   # .csv export = last ~3 years, no date filter
     "url": ELHUB_API + "/municipalities.csv?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR"},
    {"source": "elhub", "dataset": "norgespris_consumption_per_group_eac_mba", # kWh with/without Norgespris per price area
     "mode": "snapshot", "sep": ",", "required": False,                  # unverified dataset name -> optional
     "url": ELHUB_API + "/price-areas.csv?dataset=NORGESPRIS_CONSUMPTION_PER_GROUP_EAC_MBA"},
    {"source": "elhub", "dataset": "norgespris_count_per_municipality_consumption_group",  # Norgespris meters per municipality
     "mode": "snapshot", "sep": ",", "required": False,
     "url": "https://data.elhub.no/download/norgespris_count_per_municipality_consumption_group/"
            "norgespris_count_per_municipality_consumption_group-all-en-0000-00-00.csv"},
    {"source": "ssb", "dataset": "population_07459",                          # population per municipality
     "mode": "snapshot", "sep": ";", "required": True, "url": SSB_URL},
    {"source": "met", "dataset": "seklima_daily_mean_temperature",            # Seklima export (manual)
     "mode": "manual", "sep": ";", "required": True},
    {"source": "met", "dataset": "frost_stations",                            # station -> municipality (Frost API)
     "mode": "snapshot", "sep": None, "required": False, "auth": "frost", "ext": "json",
     "url": "https://frost.met.no/sources/v0.jsonld?types=SensorSystem&country=NO"},
]

MANIFEST_COLUMNS = ["run_id", "env", "source", "dataset", "period", "origin",  # columns of _manifest.csv
                    "original_filename", "bronze_path", "ingested_at_utc", "size_bytes",
                    "sha256", "header", "schema_status", "status"]


def sha256_of(path):
    """Return the SHA-256 checksum of a file."""
    h = hashlib.sha256()                                       # empty checksum object
    with open(path, "rb") as f:                                # binary mode: we hash the exact bytes
        for block in iter(lambda: f.read(1024 * 1024), b""):   # read 1 MB at a time until end of file
            h.update(block)                                    # add the block to the checksum
    return h.hexdigest()                                       # 64-character hex string


def content_sha256(path):
    """Checksum of the data: for a ZIP the files inside (in name order), otherwise the file itself."""
    if not zipfile.is_zipfile(path):                           # normal file (CSV/JSON)
        return sha256_of(path)
    h = hashlib.sha256()
    with zipfile.ZipFile(path) as zf:                          # open the archive
        for name in sorted(zf.namelist()):                     # same order every time
            with zf.open(name) as member:                      # read one file inside the ZIP
                for block in iter(lambda: member.read(1024 * 1024), b""):  # 1 MB at a time
                    h.update(block)
    return h.hexdigest()


def read_header(path):
    """Return the first line of a file, or of the first CSV inside a ZIP (without BOM and line break)."""
    if zipfile.is_zipfile(path):                               # ZIP -> look at the first CSV inside
        with zipfile.ZipFile(path) as zf:
            csvs = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))  # CSV members only
            if not csvs:                                       # a ZIP without CSV: nothing to compare
                return ""
            with zf.open(csvs[0]) as member:
                first = member.readline(2000)                  # first line of the first CSV
    else:
        with open(path, "rb") as f:                            # binary mode, so odd characters cannot crash us
            first = f.readline(2000)                           # first line only, max 2000 bytes (JSON has long lines)
    return first.decode("utf-8-sig", errors="replace").strip() # utf-8-sig removes the BOM, strip removes \r\n


def read_manifest():
    """Return all rows of the manifest as a list of dicts (empty list on the first run)."""
    if not os.path.exists(MANIFEST_PATH):                      # first run: no manifest yet
        return []
    with open(MANIFEST_PATH, newline="", encoding="utf-8") as f:  # newline="" is required by the csv module
        return list(csv.DictReader(f))                         # one dict per row, keys = column names


def append_manifest(row):
    """Append one row to the manifest (writes the column names if the file is new)."""
    os.makedirs(BRONZE_DIR, exist_ok=True)                     # make sure the bronze folder exists
    is_new = not os.path.exists(MANIFEST_PATH)                 # do we need to write the column names?
    with open(MANIFEST_PATH, "a", newline="", encoding="utf-8") as f:  # "a" = append, never overwrite
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS, restval="")  # missing keys become ""
        if is_new:
            writer.writeheader()                               # column names first in a new manifest
        writer.writerow(row)                                   # this file's metadata


def month_list(start, end):
    """List of 'YYYY-MM' strings from start to end, both included."""
    y, m = map(int, start.split("-"))                          # start year and month as numbers
    ey, em = map(int, end.split("-"))                          # end year and month as numbers
    months = []
    while (y, m) <= (ey, em):                                  # tuples compare year first, then month
        months.append(f"{y:04d}-{m:02d}")                      # e.g. "2025-09"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)           # next month (December -> January next year)
    return months


def next_month(month):
    """'2025-12' -> '2026-01'."""
    y, m = map(int, month.split("-"))                          # split into numbers
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)               # one month forward
    return f"{y:04d}-{m:02d}"


def last_complete_month(today=None):
    """The month before the current one, e.g. '2026-10' in November 2026."""
    today = today or date.today()                              # allow a fixed date in tests
    y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)  # one month back
    return f"{y:04d}-{m:02d}"


def gunzip_if_needed(path):
    """If the file is gzip-compressed, replace it with the unpacked content."""
    with open(path, "rb") as f:
        if f.read(2) != b"\x1f\x8b":                         # gzip files always start with these two bytes
            return                                             # not gzip -> nothing to do
    with gzip.open(path, "rb") as src, open(path + ".unpacked", "wb") as dst:  # read compressed, write plain
        shutil.copyfileobj(src, dst)                           # stream, so big files are fine
    os.replace(path + ".unpacked", path)                       # swap the unpacked file in
    logger.info(f"  unpacked gzip: {os.path.basename(path)}")


def download(url, target_path, headers=None, attempts=3):
    """Download url to target_path; retry a few times, raise the last error if all attempts fail."""
    all_headers = {"User-Agent": "DAT535-student-project"}     # say politely who we are
    all_headers.update(headers or {})                          # add e.g. an Authorization header
    for attempt in range(1, attempts + 1):                     # attempt 1, 2, 3
        try:
            req = urllib.request.Request(url, headers=all_headers)                    # build the request
            with urllib.request.urlopen(req, timeout=300) as resp, open(target_path, "wb") as out:  # max 5 min
                shutil.copyfileobj(resp, out)                  # stream the response to the file
            gunzip_if_needed(target_path)                      # undo gzip transfer compression (data.elhub.no)
            return                                             # success -> stop retrying
        except Exception as e:                                 # network error, HTTP 400/404/500, timeout ...
            logger.warning(f"Download attempt {attempt}/{attempts} failed for {url}: {e}")
            if attempt == attempts:                            # the last attempt also failed ...
                raise                                          # ... let the caller record the failure
            time.sleep(5 * attempt)                            # wait 5 s, then 10 s


def auth_headers(source_cfg):
    """Extra HTTP headers for sources that need a login (only Frost). None = cannot download."""
    if source_cfg.get("auth") != "frost":                      # all other sources are open
        return {}
    client_id = os.environ.get("FROST_CLIENT_ID")              # from GitHub Secrets or your shell
    if not client_id:                                          # no ID -> skip this source
        return None
    token = base64.b64encode(f"{client_id}:".encode()).decode()  # Basic auth: "<client_id>:" (empty password)
    return {"Authorization": f"Basic {token}"}


def ingest_file(state, src_path, source, dataset, period, origin, original_filename):
    """Copy one file unchanged into Bronze unless an identical file is already there."""
    checksum = content_sha256(src_path)                         # fingerprint of the content (inside a ZIP: the files)
    header = read_header(src_path)                              # column-names line
    earlier = [r for r in state["manifest"]                     # earlier files of the SAME dataset ...
               if r["dataset"] == dataset and r["status"] == "ingested"]  # ... that were actually stored

    row = {"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,  # metadata for the manifest
           "period": period, "origin": origin, "original_filename": original_filename,
           "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "size_bytes": os.path.getsize(src_path), "sha256": checksum, "header": header}

    if any(r["sha256"] == checksum for r in earlier):           # exact same content seen before?
        row["status"] = "skipped_duplicate"                     # yes -> do not store it again
        state["counts"]["skipped_duplicate"] += 1
        logger.info(f"  skip (already ingested): {dataset} {period}")
    else:
        if not earlier:                                         # first file ever for this dataset
            row["schema_status"] = "first_file"
        elif earlier[-1]["header"] == header:                   # same columns as the newest earlier file
            row["schema_status"] = "ok"
        else:                                                   # columns changed -> keep the file, but flag it
            row["schema_status"] = "changed"
            logger.warning(f"  HEADER CHANGED for {dataset}: check before Silver!")
        is_zip = zipfile.is_zipfile(src_path)                                 # Elhub exports are ZIPs
        if is_zip and not original_filename.lower().endswith(".zip"):         # give it the correct file ending
            original_filename = os.path.splitext(original_filename)[0] + ".zip"
        target_dir = os.path.join(BRONZE_DIR, source, dataset, period)        # e.g. bronze/elhub/<ds>/snapshot
        os.makedirs(target_dir, exist_ok=True)                                # create the folder if missing
        target = os.path.join(target_dir, f"{state['run_id']}__{original_filename}")  # run_id keeps versions apart
        shutil.copy2(src_path, target)                                        # byte-for-byte copy of the original
        if is_zip:                                                            # unpack a copy for Spark
            with zipfile.ZipFile(target) as zf:
                zf.extractall(target[:-len(".zip")] + "_unzipped")            # <run_id>__<name>_unzipped/part-....csv
        row["bronze_path"] = os.path.relpath(target, BRONZE_DIR)              # path relative to bronze/
        row["status"] = "ingested"
        state["counts"]["ingested"] += 1
        logger.info(f"  ingested: {row['bronze_path']}")

    append_manifest(row)                                        # write the line to disk right away
    state["manifest"].append(row)                               # and remember it for the rest of the run


def download_and_ingest(state, cfg, url, period, filename):
    """Download url to a temp file and ingest it; record failures in the manifest."""
    source, dataset = cfg["source"], cfg["dataset"]
    headers = auth_headers(cfg)                                  # {} for open sources, None if login is missing
    if headers is None:                                          # e.g. FROST_CLIENT_ID not set
        logger.info(f"  skip {dataset}: no credentials (set FROST_CLIENT_ID to enable)")
        return
    tmp_path = os.path.join(state["tmp_dir"], filename)          # temp file in bronze/_tmp/<run_id>/
    try:
        download(url, tmp_path, headers)                         # may raise after 3 failed attempts
        ingest_file(state, tmp_path, source, dataset, period, url, filename)  # checks + copy + manifest
    except Exception as e:                                       # download (or copy) failed
        level = "failed" if cfg["required"] else "warning"       # optional sources do not fail the run
        state["counts"][level] += 1
        append_manifest({"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,
                         "period": period, "origin": url, "original_filename": filename,
                         "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                         "status": f"{level}: {type(e).__name__} {e}"[:200]})  # e.g. "failed: HTTPError ..."
        logger.error(f"  {level.upper()}: {url} ({e})")
    finally:
        if os.path.exists(tmp_path):                             # always clean up the temp file
            os.remove(tmp_path)


def months_to_download(state, cfg):
    """Months missing in Bronze, plus the newest months again (to catch revised data)."""
    start, end = MONTH_RANGE[ENV]                                # range for dev or prod
    start = max(start, cfg.get("start", start))                  # some datasets start later ("YYYY-MM" sorts as text)
    months = month_list(start, end or last_complete_month())     # None = up to the last complete month
    have = {r["period"] for r in state["manifest"]               # months we already stored
            if r["dataset"] == cfg["dataset"] and r["status"] == "ingested"}
    recent = set(months[-REDOWNLOAD_RECENT_MONTHS:])             # the newest N months
    return [m for m in months if m not in have or m in recent]   # missing OR recent


def run_bronze():
    """Run Bronze ingestion for all sources and return a summary dict."""
    start_time = time.time()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")  # e.g. 20261109T201500Z
    state = {"run_id": run_id,                                 # shared state for the helper functions
             "tmp_dir": os.path.join(BRONZE_DIR, "_tmp", run_id),
             "manifest": read_manifest(),                      # everything ingested in earlier runs
             "counts": {"ingested": 0, "skipped_duplicate": 0, "warning": 0, "failed": 0}}
    os.makedirs(state["tmp_dir"], exist_ok=True)
    logger.info(f"Bronze run {run_id} (env={ENV}) -> {BRONZE_DIR}")

    for cfg in SOURCES:                                        # one dataset at a time
        source, dataset, mode = cfg["source"], cfg["dataset"], cfg["mode"]
        logger.info(f"[{source}/{dataset}] mode={mode}")
        ext = cfg.get("ext", "csv")                            # file extension (Frost gives JSON)

        if mode == "monthly":                                  # one API request per month
            for month in months_to_download(state, cfg):
                url = cfg["url"].format(start=f"{month}-01",                 # first day of the month
                                        end=f"{next_month(month)}-01")       # first day of next month
                download_and_ingest(state, cfg, url, month, f"{dataset}_{month}.{ext}")
        elif mode == "snapshot":                               # one file with everything
            download_and_ingest(state, cfg, cfg["url"], "snapshot", f"{dataset}.{ext}")

        landing = os.path.join(LANDING_DIR, source, dataset)   # manual files for this dataset
        os.makedirs(landing, exist_ok=True)                    # create it so you can see where files go
        for name in sorted(os.listdir(landing)):               # every file in the landing folder
            if name.lower().endswith((".csv", ".json", ".zip")):  # only data files
                ingest_file(state, os.path.join(landing, name), source, dataset,
                            "manual", f"landing/{source}/{dataset}", name)  # duplicates skipped by checksum

    shutil.rmtree(state["tmp_dir"], ignore_errors=True)        # remove the temp folder
    elapsed = time.time() - start_time
    logger.info(f"Bronze done in {elapsed:.1f}s: {state['counts']}")
    status = "success" if state["counts"]["failed"] == 0 else "failed"  # a failed REQUIRED source -> failed run
    return {"status": status, "elapsed_time": elapsed, **state["counts"]}


if __name__ == "__main__":
    result = run_bronze()                                      # run all sources
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
