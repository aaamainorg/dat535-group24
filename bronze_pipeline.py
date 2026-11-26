#!/usr/bin/env python3
"""
DAT535 Group 24 - ingest raw files into the landing folder
==========================================================

GENERATED from the code cells in sections 3.1-3.9 of
notebooks/01_bronze.ipynb. The notebook explains every line;
this file is what GitHub Actions runs. If you change the logic, change the
notebook cell and copy it here, so the two stay identical.

Credentials: FROST_CLIENT_ID from .env (repo root) or from the environment.

Load mode (NORGESPRIS_LOAD_MODE in .env): "delta" (default) downloads only the
newest complete month(s) of the time series after the first full load; "full"
downloads the whole files every run (the old behaviour).

Usage:
    python run_pipeline.py bronze      (recommended)
    python bronze_pipeline.py
"""

import base64                                  # build the HTTP Basic-auth header for the Frost API
import csv                                     # read and write the manifest file (handles commas/quotes in text)
import gzip                                    # data.elhub.no sends files gzip-compressed
import hashlib                                 # SHA-256 checksums to recognise identical files
import json                                    # read the Frost station list to know which stations to ask for
import logging                                 # log messages that also show up in GitHub Actions
import os                                      # environment variables and file paths
import shutil                                  # copy files and delete temp folders
import time                                    # measure run time and wait between download retries
import urllib.error                            # tells us the HTTP status code when a request fails (e.g. 404)
import urllib.request                          # download files over HTTPS (standard library, no pip install needed)
import zipfile                                 # Elhub's ".csv" exports are really ZIP files with a CSV inside
from datetime import datetime, timezone, date  # timestamps (always UTC) and month calculations

logging.basicConfig(level=logging.INFO,                                        # show INFO and above
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")  # time - logger - level - text
logger = logging.getLogger("bronze")                                          # named logger for this layer


def load_env_file(candidates=(".env", os.path.join("..", ".env"))):  # look for .env in the repo root (also from notebooks/)
    """Read KEY=value lines from the first .env file found into os.environ (does not overwrite existing values)."""
    for path in candidates:                                   # repo root seen from the root or from notebooks/
        if not os.path.exists(path):                            # this candidate does not exist ...
            continue                                            # ... try the next one
        loaded = []                                             # names of the keys we set (for the log)
        with open(path, encoding="utf-8") as f:                 # open the .env file as text
            for line in f:                                      # one KEY=value per line
                line = line.strip()                           # remove spaces and the line break
                if not line or line.startswith("#") or "=" not in line:  # skip empty lines and comments
                    continue                                    # skip this line
                key, value = line.split("=", 1)               # split at the FIRST "=" only
                key = key.replace("export ", "").strip()      # allow "export KEY=value" too
                value = value.strip().strip('"').strip("'")   # remove spaces and surrounding quotes
                if key and key not in os.environ:             # real environment variables win (GitHub secrets)
                    os.environ[key] = value                     # make the value visible to the code
                    loaded.append(key)                          # remember the name (never log the value)
        logger.info(f"Loaded {loaded or 'no new keys'} from {os.path.abspath(path)}")  # names only, never values
        return                                                  # stop after the first .env found
    logger.info("No .env file found - using environment variables only")  # no .env: GitHub Actions passes secrets as env vars

load_env_file()                                               # run it once when the code is loaded

ENV = os.environ.get("NORGESPRIS_ENV", "dev")                 # "dev" or "prod"; default dev when run by hand

LANDING_ROOT = os.environ.get("NORGESPRIS_LANDING_ROOT",      # where the raw files are stored ...
                              "/tmp/norgespris-landing")      # ... default: a folder in /tmp
LANDING_DIR = os.path.join(LANDING_ROOT, ENV)                 # /tmp/norgespris-landing/dev or .../prod
MANUAL_DIR = os.path.join(LANDING_DIR, "_manual")             # drop manually exported files here
MANIFEST_PATH = os.path.join(LANDING_DIR, "_manifest.csv")    # one line per file we have seen

REDOWNLOAD_RECENT_MONTHS = 2                                  # always re-fetch the newest months (MET corrects recent data)
FROST_STATIONS_PER_REQUEST = 100                              # stations per observations request (keeps answers small)
LOAD_MODE = os.environ.get("NORGESPRIS_LOAD_MODE", "delta").strip().lower()  # "delta" = only new months, "full" = whole files
DELTA_MONTHS = int(os.environ.get("NORGESPRIS_DELTA_MONTHS", "1"))  # delta: always (re)load at least this many newest months


def month_range():                                              # the period for dev or prod from .env
    """(start, end) months for the current environment from .env; end is None = up to the last complete month."""
    prefix = f"NORGESPRIS_{ENV.upper()}"                       # NORGESPRIS_DEV or NORGESPRIS_PROD
    start = os.environ.get(f"{prefix}_START_MONTH", "").strip()  # e.g. "2025-09"
    end = os.environ.get(f"{prefix}_END_MONTH", "").strip() or None  # empty -> None
    for key, value in ((f"{prefix}_START_MONTH", start), (f"{prefix}_END_MONTH", end)):  # check both settings the same way
        if value is None and key.endswith("END_MONTH"):        # an empty end month is allowed
            continue                                            # empty end month is fine -> next setting
        if len(value) != 7 or value[4] != "-" or not (value[:4] + value[5:]).isdigit():  # must look like YYYY-MM
            raise ValueError(f"{key} must be set as YYYY-MM in .env (or as environment variable), got '{value}'")  # clear message instead of a crash later
        try:                                                    # check it is a real month ...
            date.fromisoformat(f"{value}-01")                   # ... e.g. 2025-13 fails here
        except ValueError as e:                                 # not a calendar month
            raise ValueError(f"{key} must be a valid calendar month (YYYY-MM), got '{value}'") from e  # explain which setting is wrong
    return start, end                                           # e.g. ('2024-01', None)


ELHUB_API = "https://api.elhub.no/energy-data/v0"             # Elhub Energy Data API (open, no login)
FROST_API = "https://frost.met.no"                            # MET Norway's Frost API (needs a client ID)
FROST_ELEMENT = "mean(air_temperature%20P1D)"                 # daily mean air temperature (%20 = space)

SSB_URL = ("https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"      # SSB table 07459, Norwegian labels
           "&valueCodes[Region]=*&codelist[Region]=agg_KommSummer"             # all municipalities, today's structure
           "&outputValues[Region]=aggregated"                                  # sums for merged municipalities
           "&valueCodes[ContentsCode]=*&valueCodes[Tid]=from(2023)"            # population, years 2023 ->
           "&outputFormat=csv&outputFormatParams=separatorsemicolon,usecodesandtexts")  # ';' + code AND name ('3101 Halden')
# Sex and age are left out of the query -> SSB sums them ("elimination"), so we get total population.

SOURCES = [                                                                    # the datasets the pipeline handles
    {"source": "elhub", "dataset": "consumption_per_group_municipality_hour",  # hourly kWh per municipality and group
     "mode": "snapshot", "sep": ";", "required": True,                         # full: .csv export = last ~3 years
     "url": ELHUB_API + "/municipalities.csv?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR",  # Elhub API, CSV export (a ZIP)
     "delta_url": "https://data.elhub.no/download/consumption_per_group_municipality_hour/"  # delta: Elhub's file ...
                  "consumption_per_group_municipality_hour-all-en-{month}-00.csv"},  # ... of one month (00 = all days)
    {"source": "elhub", "dataset": "norgespris_consumption_per_group_eac_mba", # kWh with/without Norgespris per price area
     "mode": "snapshot", "sep": ",", "required": False,         # comma-separated, optional dataset
     "url": ELHUB_API + "/price-areas.csv?dataset=NORGESPRIS_CONSUMPTION_PER_GROUP_EAC_MBA"},  # Elhub API, price-area export (small: always full)
    {"source": "elhub", "dataset": "norgespris_count_per_municipality_consumption_group",  # Norgespris meters per municipality
     "mode": "snapshot", "sep": ",", "required": False,         # comma-separated, optional dataset
     "url": "https://data.elhub.no/download/norgespris_count_per_municipality_consumption_group/"  # data.elhub.no download ...
            "norgespris_count_per_municipality_consumption_group-all-en-0000-00-00.csv",  # ... the 'all' file (all days so far)
     "delta_url": "https://data.elhub.no/download/norgespris_count_per_municipality_consumption_group/"  # delta: the file ...
                  "norgespris_count_per_municipality_consumption_group-all-en-{month}-00.csv"},  # ... of one month (00 = all days)
    {"source": "ssb", "dataset": "population_07459",                          # population per municipality
     "mode": "snapshot", "sep": ";", "required": True, "url": SSB_URL},  # semicolon CSV, required
    {"source": "met", "dataset": "frost_stations",                            # 1) station -> municipality
     "mode": "snapshot", "sep": None, "required": True, "auth": "frost", "ext": "json",  # JSON, required, needs a Frost login
     "url": FROST_API + "/sources/v0.jsonld?types=SensorSystem&validtime=2023-01-01/2027-01-01"},  # all stations valid in our years
    {"source": "met", "dataset": "frost_timeseries",                          # 2) which stations have daily mean temp
     "mode": "snapshot", "sep": None, "required": True, "auth": "frost", "ext": "json",  # JSON, required, needs a Frost login
     "url": FROST_API + "/observations/availableTimeSeries/v0.jsonld?elements=" + FROST_ELEMENT  # time series with daily mean temperature ...
                      + "&referencetime=2023-11-01/2027-01-01"},  # ... that exist in our years
    {"source": "met", "dataset": "frost_observations",                        # 3) daily mean temp per station
     "mode": "frost_observations", "sep": None, "required": True, "auth": "frost", "ext": "json",  # special mode: many requests per month
     "url": FROST_API + "/observations/v0.jsonld?elements=" + FROST_ELEMENT  # observations URL with placeholders ...
                      + "&sources={sources}&referencetime={start}/{end}"},  # ... filled in per station group and month
    {"source": "met", "dataset": "seklima_daily_mean_temperature",            # manual fallback, not needed with Frost
     "mode": "manual", "sep": ";", "required": False},          # only files dropped in _manual are used
]                                                               # end of the source list

MANIFEST_COLUMNS = ["run_id", "env", "source", "dataset", "period", "origin",  # columns of _manifest.csv
                    "original_filename", "landing_path", "ingested_at_utc", "size_bytes",  # manifest columns, continued
                    "sha256", "header", "schema_status", "status"]  # manifest columns, continued


def sha256_of(path):                                            # checksum of a whole file
    """Return the SHA-256 checksum of a file."""
    h = hashlib.sha256()                                       # empty checksum object
    with open(path, "rb") as f:                                # binary mode: we hash the exact bytes
        for block in iter(lambda: f.read(1024 * 1024), b""):   # read 1 MB at a time until end of file
            h.update(block)                                    # add the block to the checksum
    return h.hexdigest()                                       # 64-character hex string


def content_sha256(path):                                       # checksum of the data (ZIP: files inside)
    """Checksum of the data: for a ZIP the files inside (in name order), otherwise the file itself."""
    if not zipfile.is_zipfile(path):                           # normal file (CSV/JSON)
        return sha256_of(path)                                  # plain file -> checksum of the file
    h = hashlib.sha256()                                        # empty checksum object
    with zipfile.ZipFile(path) as zf:                          # open the archive
        for name in sorted(zf.namelist()):                     # same order every time
            with zf.open(name) as member:                      # read one file inside the ZIP
                for block in iter(lambda: member.read(1024 * 1024), b""):  # 1 MB at a time
                    h.update(block)                             # add the block to the checksum
    return h.hexdigest()                                        # 64-character hex string


def read_header(path):                                          # column names of a CSV (for schema checks)
    """First line of a CSV (or of the first CSV inside a ZIP), without BOM and line break; '(json)' for JSON."""
    if path.lower().endswith(".json"):                         # JSON has no header line to compare
        return "(json)"                                         # marker instead of a header
    if zipfile.is_zipfile(path):                               # ZIP -> look at the first CSV inside
        with zipfile.ZipFile(path) as zf:                       # open the ZIP archive
            csvs = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))  # CSV members only
            if not csvs:                                       # a ZIP without CSV: nothing to compare
                return ""                                       # empty header
            with zf.open(csvs[0]) as member:                    # open the first CSV inside the ZIP
                first = member.readline(2000)                  # first line of the first CSV
    else:                                                       # not a ZIP
        with open(path, "rb") as f:                            # binary mode, so odd characters cannot crash us
            first = f.readline(2000)                           # first line only, max 2000 bytes
    return first.decode("utf-8-sig", errors="replace").strip() # utf-8-sig removes the BOM, strip removes \r\n


def read_manifest():                                            # read all manifest rows
    """Return all rows of the manifest as a list of dicts (empty list on the first run)."""
    if not os.path.exists(MANIFEST_PATH):                      # first run: no manifest yet
        return []                                               # no rows yet
    with open(MANIFEST_PATH, newline="", encoding="utf-8") as f:  # newline="" is required by the csv module
        return list(csv.DictReader(f))                         # one dict per row, keys = column names


def append_manifest(row):                                       # add one manifest row
    """Append one row to the manifest (writes the column names if the file is new)."""
    os.makedirs(LANDING_DIR, exist_ok=True)                    # make sure the landing folder exists
    is_new = not os.path.exists(MANIFEST_PATH)                 # do we need to write the column names?
    with open(MANIFEST_PATH, "a", newline="", encoding="utf-8") as f:  # "a" = append, never overwrite
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS, restval="")  # missing keys become ""
        if is_new:                                              # new file ...
            writer.writeheader()                               # column names first in a new manifest
        writer.writerow(row)                                   # this file's metadata


def month_list(start, end):                                     # months between two YYYY-MM values
    """List of 'YYYY-MM' strings from start to end, both included."""
    y, m = map(int, start.split("-"))                          # start year and month as numbers
    ey, em = map(int, end.split("-"))                          # end year and month as numbers
    months = []                                                 # collected months
    while (y, m) <= (ey, em):                                  # tuples compare year first, then month
        months.append(f"{y:04d}-{m:02d}")                      # e.g. "2025-09"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)           # next month (December -> January next year)
    return months                                               # e.g. ['2025-09', '2025-10']


def next_month(month):                                          # the month after a YYYY-MM value
    """'2025-12' -> '2026-01'."""
    y, m = map(int, month.split("-"))                          # split into numbers
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)               # one month forward
    return f"{y:04d}-{m:02d}"                                   # back to 'YYYY-MM'


def last_complete_month(today=None):                            # the newest month that is over
    """The month before the current one, e.g. '2026-10' in November 2026."""
    today = today or date.today()                              # allow a fixed date in tests
    y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)  # one month back
    return f"{y:04d}-{m:02d}"                                   # back to 'YYYY-MM'


def delta_url(cfg, month):                                      # the URL of one month of a source
    """'...-all-en-{month}-00.csv' -> '...-all-en-2026-10-00.csv' (data.elhub.no: day 00 = the whole month)."""
    return cfg["delta_url"].format(month=month)                 # fill in the month


def month_loaded(row):                                          # newest complete month a manifest row covers
    """The month of a monthly file; for a full file the last complete month on the day it was downloaded."""
    if len(row["period"]) == 7 and row["period"][4] == "-":     # a monthly file: its period is 'YYYY-MM'
        return row["period"]                                    # e.g. '2026-10'
    return last_complete_month(date.fromisoformat(row["ingested_at_utc"][:10]))  # full file: complete up to the month before


def delta_months(state, dataset):                               # months a delta run (re)loads for a dataset
    """Months from the newest month already in landing to the end of the period (at least the last DELTA_MONTHS).
    None = nothing in landing yet -> the caller does a full load first."""
    start, end = month_range()                                  # the period from .env
    months = month_list(start, end or last_complete_month())    # every month of the period
    loaded = [r for r in state["manifest"] if r["dataset"] == dataset  # earlier files of this dataset ...
              and r["status"] in ("ingested", "skipped_duplicate", "no_data")]  # ... that are in landing (or known empty)
    if not loaded or not months:                                # first run (or an empty period) ...
        return None                                             # ... -> full load
    newest = max(month_loaded(r) for r in loaded)               # high-water mark: the newest month we already have
    last_n = months[-max(1, min(DELTA_MONTHS, len(months)))]    # first of the last DELTA_MONTHS months of the period
    first = min(newest, last_n)                                 # reload the high-water month too (sources correct recent data)
    return [m for m in months if m >= first]                    # e.g. ['2026-10'] in November 2026


def gunzip_if_needed(path):                                     # undo gzip compression of a downloaded file
    """If the file is gzip-compressed, replace it with the unpacked content."""
    with open(path, "rb") as f:                                 # look at the first two bytes
        if f.read(2) != b"\x1f\x8b":                           # gzip files always start with these two bytes
            return                                             # not gzip -> nothing to do
    with gzip.open(path, "rb") as src, open(path + ".unpacked", "wb") as dst:  # read compressed, write plain
        shutil.copyfileobj(src, dst)                           # stream, so big files are fine
    os.replace(path + ".unpacked", path)                       # swap the unpacked file in
    logger.info(f"  unpacked gzip: {os.path.basename(path)}")   # note it in the log


def auth_headers(source_cfg):                                   # HTTP headers needed for a source
    """Extra HTTP headers for sources that need a login. {} = open source, None = login missing."""
    if source_cfg.get("auth") != "frost":                      # Elhub and SSB are open
        return {}                                               # no login needed
    client_id = os.environ.get("FROST_CLIENT_ID", "").strip()  # from .env or a GitHub secret
    if not client_id or client_id.startswith("your-"):         # missing, or still the placeholder from .env
        return None                                             # None = login missing (caller reports it)
    token = base64.b64encode(f"{client_id}:".encode()).decode()  # "<client id>:" = empty password (Frost docs)
    return {"Authorization": f"Basic {token}"}                  # HTTP Basic auth header


def download(url, target_path, headers=None, attempts=3):       # download one URL with retries
    """Download url to target_path; retry server/network errors, raise the last error if all attempts fail."""
    all_headers = {"User-Agent": "DAT535-group24-student-project"}  # say politely who we are
    all_headers.update(headers or {})                          # add e.g. the Authorization header
    for attempt in range(1, attempts + 1):                     # attempt 1, 2, 3
        try:                                                    # try the download
            req = urllib.request.Request(url, headers=all_headers)                    # build the request
            with urllib.request.urlopen(req, timeout=300) as resp, open(target_path, "wb") as out:  # max 5 min
                shutil.copyfileobj(resp, out)                  # stream the response to the file
            gunzip_if_needed(target_path)                      # undo gzip transfer compression (data.elhub.no)
            return                                             # success -> stop retrying
        except urllib.error.HTTPError as e:                    # the server answered with an error code
            if e.code in (400, 401, 403, 404):                 # our request is wrong / no access / no data
                raise                                          # -> retrying will not help
            error = e                                          # 429, 500, 503 ... -> worth another try
        except Exception as e:                                 # network problem, timeout ...
            error = e                                           # remember the error and retry
        logger.warning(f"Download attempt {attempt}/{attempts} failed for {url}: {error}")  # log each failed attempt
        if attempt == attempts:                                # the last attempt also failed ...
            raise error                                        # ... let the caller record the failure
        time.sleep(5 * attempt)                                # wait 5 s, then 10 s


def ingest_file(state, src_path, source, dataset, period, origin, original_filename):  # store one file in landing (or skip it)
    """Store one file unchanged in the landing folder unless an identical file is already there."""
    checksum = content_sha256(src_path)                         # fingerprint of the content (inside a ZIP: the files)
    header = read_header(src_path)                              # column-names line, or "(json)"
    earlier = [r for r in state["manifest"]                     # earlier files of the SAME dataset ...
               if r["dataset"] == dataset and r["status"] == "ingested"]  # ... that were actually stored

    row = {"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,  # metadata for the manifest
           "period": period, "origin": origin, "original_filename": original_filename,  # manifest columns, continued
           "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),  # time of ingestion (UTC)
           "size_bytes": os.path.getsize(src_path), "sha256": checksum, "header": header}  # size, checksum and header

    if any(r["sha256"] == checksum for r in earlier):           # exact same content seen before?
        row["landing_path"] = next(r["landing_path"] for r in reversed(earlier) if r["sha256"] == checksum)  # point to the stored copy
        row["status"] = "skipped_duplicate"                     # yes -> do not store it again
        state["counts"]["skipped_duplicate"] += 1               # count skipped files
        logger.info(f"  skip (already ingested): {dataset} {period} {original_filename}")  # log the skip
    else:                                                       # new content -> store it
        if not earlier:                                         # first file ever for this dataset
            row["schema_status"] = "first_file"                 # nothing to compare with
        elif earlier[-1]["header"] == header:                   # same columns as the newest earlier file
            row["schema_status"] = "ok"                         # header unchanged
        else:                                                   # columns changed -> keep the file, but flag it
            row["schema_status"] = "changed"                    # header changed -> Silver must check
            logger.warning(f"  HEADER CHANGED for {dataset}: check before Silver!")  # warn in the log
        is_zip = zipfile.is_zipfile(src_path)                                 # Elhub exports are ZIPs
        stored_name = original_filename                         # file name to store under
        if is_zip and not stored_name.lower().endswith(".zip"):               # give it the correct file ending
            stored_name = os.path.splitext(stored_name)[0] + ".zip"  # replace the ending with .zip
        target_dir = os.path.join(LANDING_DIR, source, dataset, period)       # e.g. <landing>/met/frost_observations/2025-10
        os.makedirs(target_dir, exist_ok=True)                                # create the folder if missing
        target = os.path.join(target_dir, f"{state['run_id']}__{stored_name}")  # run_id keeps versions apart
        shutil.copy2(src_path, target)                                        # byte-for-byte copy of the original
        if is_zip:                                                            # unpack a copy for Spark
            with zipfile.ZipFile(target) as zf:                 # open the stored ZIP
                zf.extractall(target[:-len(".zip")] + "_unzipped")            # <run_id>__<name>_unzipped/part-....csv
        row["landing_path"] = os.path.relpath(target, LANDING_DIR)            # path relative to the landing folder
        row["status"] = "ingested"                              # stored
        state["counts"]["ingested"] += 1                        # count stored files
        logger.info(f"  ingested: {row['landing_path']}")       # log where it went

    append_manifest(row)                                        # write the line to disk right away
    state["manifest"].append(row)                               # and remember it for the rest of the run


def download_and_ingest(state, cfg, url, period, filename, delta=False):  # download one file, then ingest it
    """Download url to a temp file and ingest it; record every outcome in the manifest.
    Returns "stored", "no_data", "failed" or - only for a month file (delta=True) that is missing (400/404)
    or has other columns than the full file - "rejected", so the caller can download the full file instead."""
    source, dataset = cfg["source"], cfg["dataset"]             # names for the manifest and folders
    level = "failed" if cfg["required"] else "warning"           # how bad a failure is for this source
    base_row = {"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,  # manifest row for failures ...
                "period": period, "origin": url, "original_filename": filename,  # ... where it came from ...
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}  # ... and when
    headers = auth_headers(cfg)                                  # {} for open sources, None if login is missing
    if headers is None:                                          # Frost without a client ID
        state["counts"][level] += 1                             # count the failure
        append_manifest(dict(base_row, status=f"{level}: FROST_CLIENT_ID missing (set it in .env)"))  # record it in the manifest
        logger.error(f"  {level.upper()}: {dataset} needs FROST_CLIENT_ID (in .env or as GitHub secret)")  # explain how to fix it
        return "failed"                                         # nothing to download
    tmp_path = os.path.join(state["tmp_dir"], filename)          # arrives in <landing>/_incoming/<run_id>/ first
    outcome = "failed"                                          # changed below when it works
    try:                                                        # download + store; errors handled below
        download(url, tmp_path, headers)                         # may raise after failed attempts
        if delta:                                               # a month file must have the same columns as the full file
            earlier = [r for r in state["manifest"] if r["dataset"] == dataset and r["status"] == "ingested"]  # stored files
            if earlier and earlier[-1]["header"] != read_header(tmp_path):  # other columns (or separator) -> do not mix them
                state["counts"]["warning"] += 1                 # a warning: the caller loads the full file instead
                append_manifest(dict(base_row, status="warning: delta file has other columns - full download instead"))  # record it
                logger.warning(f"  delta file {filename} has other columns than the full file - full download instead")  # explain
                return "rejected"                               # (the finally block below still deletes the temp file)
        ingest_file(state, tmp_path, source, dataset, period, url, filename)  # checks + copy + manifest
        outcome = "stored"                                      # in landing now (or a duplicate of a stored file)
    except urllib.error.HTTPError as e:                         # the server answered with an error
        if e.code == 404 and cfg["mode"] == "frost_observations":  # Frost: "no data for these stations/month"
            state["counts"]["no_data"] += 1                     # count 'no data'
            row = dict(base_row, status="no_data")              # manifest row for 'no data'
            append_manifest(row)                                # write it to the manifest file
            state["manifest"].append(row)                        # remember: do not ask again (unless recent)
            logger.info(f"  no data: {filename}")               # log it (not an error)
            outcome = "no_data"                                 # tell the caller
        elif delta and e.code in (400, 404):                    # month filter not accepted / month file not there
            state["counts"]["warning"] += 1                     # a warning, not a failure: the caller loads the full file
            append_manifest(dict(base_row, status=f"warning: delta HTTP {e.code} - full download instead"))  # record it
            logger.warning(f"  delta not available (HTTP {e.code}): {url} - downloading the full file instead")  # explain
            outcome = "rejected"                                # tell the caller
        else:                                                   # any other HTTP error
            state["counts"][level] += 1                         # count the failure
            append_manifest(dict(base_row, status=f"{level}: HTTP {e.code} {e.reason}"[:200]))  # record it in the manifest (max 200 chars)
            logger.error(f"  {level.upper()}: {url} (HTTP {e.code} {e.reason})")  # log it as an error
    except Exception as e:                                       # network error etc.
        state["counts"][level] += 1                             # count the failure
        append_manifest(dict(base_row, status=f"{level}: {type(e).__name__} {e}"[:200]))  # record it in the manifest (max 200 chars)
        logger.error(f"  {level.upper()}: {url} ({e})")         # log it as an error
    finally:                                                    # runs in every case
        if os.path.exists(tmp_path):                             # always clean up the temp file
            os.remove(tmp_path)                                 # delete the temp file
    return outcome                                              # "stored", "no_data", "rejected" or "failed"


def frost_station_ids(state):                                   # stations that have daily mean temperature
    """Station ids that have daily mean temperature, from the newest frost_timeseries file in landing."""
    rows = [r for r in state["manifest"]                         # stored availableTimeSeries files
            if r["dataset"] == "frost_timeseries"               # ... of the dataset frost_timeseries ...
            and r["status"] in ("ingested", "skipped_duplicate") and r.get("landing_path")]  # ... that were stored
    if not rows:                                                 # step 2 did not succeed (yet)
        return []                                               # no station list
    path = os.path.join(LANDING_DIR, rows[-1]["landing_path"])     # newest version
    with open(path, encoding="utf-8") as f:                     # read the JSON file
        data = json.load(f).get("data", [])                      # Frost puts the records in "data"
    return sorted({d["sourceId"].split(":")[0] for d in data})   # "SN18700:0" -> "SN18700", no duplicates


def frost_observation_requests(state, cfg):                     # all Frost observation requests to make
    """List of (url, month, filename): every month x every group of stations, minus what we already have."""
    ids = frost_station_ids(state)                               # stations to ask for
    if not ids:                                                 # no stations known
        logger.error("  no station list from frost_timeseries - cannot request observations")  # explain why nothing is requested
        return []                                               # no requests
    n = FROST_STATIONS_PER_REQUEST                              # stations per request
    groups = [ids[i:i + n] for i in range(0, len(ids), n)]       # [[SN..100 ids], [SN..], ...]
    layout = hashlib.sha256(",".join(ids).encode("utf-8")).hexdigest()[:12]  # id of this station grouping (in file names)
    start, end = month_range()                                   # period for dev or prod, from .env
    months = month_list(start, end or last_complete_month())     # None = up to the last complete month
    if LOAD_MODE == "delta":                                     # delta: only the newest month(s) ...
        months = delta_months(state, cfg["dataset"]) or months   # ... (None on the first run -> the whole period)
    recent = set(months[-REDOWNLOAD_RECENT_MONTHS:])             # always re-request the newest months
    have = {r["original_filename"] for r in state["manifest"]    # files we already got (or know are empty)
            if r["dataset"] == cfg["dataset"] and r["status"] in ("ingested", "skipped_duplicate", "no_data")}  # ... of this dataset
    requests = []                                               # collected (url, month, filename)
    for month in months:                                        # every month of the period ...
        for g, group in enumerate(groups, start=1):             # ... times every group of stations
            filename = f"frost_obs_{month}_l{layout}_g{g:02d}.json"  # file name: month, grouping, group number
            if filename in have and month not in recent:         # already stored and not recent -> skip
                continue                                        # skip it
            url = cfg["url"].format(sources=",".join(group),     # SN18700,SN18950,...
                                    start=f"{month}-01",          # first day of the month
                                    end=f"{next_month(month)}-01")  # first day of next month (end not included)
            requests.append((url, month, filename))             # add the request
    logger.info(f"  {len(ids)} stations in {len(groups)} groups -> {len(requests)} requests")  # how many requests will be made
    return requests                                             # the list of requests


def run_bronze():                                               # the whole landing stage
    """Run Bronze ingestion for all sources and return a summary dict."""
    start_time = time.time()                                    # start time
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")  # microseconds keep rapid runs apart
    state = {"run_id": run_id,                                 # shared state for the helper functions
             "tmp_dir": os.path.join(LANDING_DIR, "_incoming", run_id),  # downloads arrive here first
             "manifest": read_manifest(),                      # everything ingested in earlier runs
             "counts": {"ingested": 0, "skipped_duplicate": 0, "no_data": 0, "warning": 0, "failed": 0}}  # outcome counters
    if LOAD_MODE not in ("full", "delta"):                      # only two allowed values
        raise ValueError(f"NORGESPRIS_LOAD_MODE must be 'full' or 'delta', got '{LOAD_MODE}'")  # clear message
    os.makedirs(state["tmp_dir"], exist_ok=True)                # create the temp download folder
    logger.info(f"Landing run {run_id} (env={ENV}, load mode={LOAD_MODE}) -> {LANDING_DIR}")  # log the start

    for cfg in SOURCES:                                        # one dataset at a time, in registry order
        source, dataset, mode = cfg["source"], cfg["dataset"], cfg["mode"]  # settings of this dataset
        logger.info(f"[{source}/{dataset}] mode={mode}")        # log which dataset
        ext = cfg.get("ext", "csv")                            # file extension (Frost gives JSON)

        if mode == "snapshot":                                 # one file with everything (or one per month in delta)
            months = None                                      # None = download the whole file
            if LOAD_MODE == "delta" and cfg.get("delta_url"):  # delta, and the source can give one month
                months = delta_months(state, dataset)          # None on the first run -> full load first
            if months is None:                                 # full mode, no month filter, or first run
                download_and_ingest(state, cfg, cfg["url"], "snapshot", f"{dataset}.{ext}")  # download the whole file
            else:                                              # delta: one request per month
                logger.info(f"  delta months: {months}")        # which months are (re)loaded
                for month in months:                           # usually just last month
                    outcome = download_and_ingest(state, cfg, delta_url(cfg, month), month,  # one month ...
                                                  f"{dataset}_{month}.{ext}", delta=True)  # ... stored as its own file
                    if outcome == "rejected":                  # month file missing or with other columns
                        download_and_ingest(state, cfg, cfg["url"], "snapshot", f"{dataset}.{ext}")  # -> whole file instead
                        break                                  # the whole file has every month
        elif mode == "frost_observations":                     # many small requests
            if auth_headers(cfg) is None:                      # no login -> one clear failure, not hundreds
                download_and_ingest(state, cfg, cfg["url"], "-", "frost_obs.json")  # records one clear failure
            else:                                               # login present
                for url, month, filename in frost_observation_requests(state, cfg):  # every month x station group ...
                    download_and_ingest(state, cfg, url, month, filename)  # ... download and store

        manual = os.path.join(MANUAL_DIR, source, dataset)     # manual files for this dataset
        os.makedirs(manual, exist_ok=True)                     # create it so you can see where files go
        for name in sorted(os.listdir(manual)):                # every file in the manual folder
            if name.lower().endswith((".csv", ".json", ".zip")):  # only data files
                ingest_file(state, os.path.join(manual, name), source, dataset,  # store the manual file ...
                            "manual", f"_manual/{source}/{dataset}", name)  # duplicates skipped by checksum

    shutil.rmtree(state["tmp_dir"], ignore_errors=True)        # empty the incoming folder of this run
    elapsed = time.time() - start_time                          # run time in seconds
    logger.info(f"Bronze done in {elapsed:.1f}s: {state['counts']}")  # log the totals
    status = "success" if state["counts"]["failed"] == 0 else "failed"  # a failed REQUIRED source -> failed run
    return {"status": status, "elapsed_time": elapsed, **state["counts"]}  # summary for run_pipeline.py


if __name__ == "__main__":                                      # only when run as a script
    result = run_bronze()                                      # run all sources
    raise SystemExit(0 if result["status"] == "success" else 1)  # non-zero exit -> red run in GitHub Actions
