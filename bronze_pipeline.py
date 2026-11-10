#!/usr/bin/env python3
"""
DAT535 Group 24 - Bronze layer (ingest and preserve)
=====================================================

GENERATED from the code cells in sections 3.1-3.9 of
notebooks/01_scope_dataset_bronze.ipynb. The notebook explains every line;
this file is what GitHub Actions runs. If you change the logic, change the
notebook cell and copy it here, so the two stay identical.

Credentials: FROST_CLIENT_ID from .env (repo root) or from the environment.

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


def load_env_file(candidates=(".env", os.path.join("..", ".env"))):
    """Read KEY=value lines from the first .env file found into os.environ (does not overwrite existing values)."""
    for path in candidates:                                   # repo root seen from the root or from notebooks/
        if not os.path.exists(path):
            continue
        loaded = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()                           # remove spaces and the line break
                if not line or line.startswith("#") or "=" not in line:  # skip empty lines and comments
                    continue
                key, value = line.split("=", 1)               # split at the FIRST "=" only
                key = key.replace("export ", "").strip()      # allow "export KEY=value" too
                value = value.strip().strip('"').strip("'")   # remove spaces and surrounding quotes
                if key and key not in os.environ:             # real environment variables win (GitHub secrets)
                    os.environ[key] = value
                    loaded.append(key)
        logger.info(f"Loaded {loaded or 'no new keys'} from {os.path.abspath(path)}")  # names only, never values
        return
    logger.info("No .env file found - using environment variables only")

load_env_file()                                               # run it once when the code is loaded

ENV = os.environ.get("NORGESPRIS_ENV", "dev")                     # "dev" or "prod"; default dev when run by hand
DATA_ROOT = os.environ.get("NORGESPRIS_OUTPUT_DIR",               # root folder chosen by the workflow ...
                           os.path.expanduser(f"~/dat535-data/{ENV}"))  # ... or ~/dat535-data/dev locally

LANDING_DIR = os.path.join(DATA_ROOT, "landing")              # put manually exported files here
BRONZE_DIR = os.path.join(DATA_ROOT, "bronze")                # raw files, never edited
MANIFEST_PATH = os.path.join(BRONZE_DIR, "_manifest.csv")     # one line per file we have seen

REDOWNLOAD_RECENT_MONTHS = 2                                  # always re-fetch the newest months (MET corrects recent data)
FROST_STATIONS_PER_REQUEST = 100                              # stations per observations request (keeps answers small)


def month_range():
    """(start, end) months for the current environment from .env; end is None = up to the last complete month."""
    prefix = f"NORGESPRIS_{ENV.upper()}"                       # NORGESPRIS_DEV or NORGESPRIS_PROD
    start = os.environ.get(f"{prefix}_START_MONTH", "").strip()  # e.g. "2025-09"
    end = os.environ.get(f"{prefix}_END_MONTH", "").strip() or None  # empty -> None
    for key, value in ((f"{prefix}_START_MONTH", start), (f"{prefix}_END_MONTH", end)):
        if value is None and key.endswith("END_MONTH"):        # an empty end month is allowed
            continue
        if len(value) != 7 or value[4] != "-" or not (value[:4] + value[5:]).isdigit():  # must look like YYYY-MM
            raise ValueError(f"{key} must be set as YYYY-MM in .env (or as environment variable), got '{value}'")
        try:
            date.fromisoformat(f"{value}-01")
        except ValueError as e:
            raise ValueError(f"{key} must be a valid calendar month (YYYY-MM), got '{value}'") from e
    return start, end


ELHUB_API = "https://api.elhub.no/energy-data/v0"             # Elhub Energy Data API (open, no login)
FROST_API = "https://frost.met.no"                            # MET Norway's Frost API (needs a client ID)
FROST_ELEMENT = "mean(air_temperature%20P1D)"                 # daily mean air temperature (%20 = space)

SSB_URL = ("https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"      # SSB table 07459, Norwegian labels
           "&valueCodes[Region]=*&codelist[Region]=agg_KommSummer"             # all municipalities, today's structure
           "&outputValues[Region]=aggregated"                                  # sums for merged municipalities
           "&valueCodes[ContentsCode]=*&valueCodes[Tid]=from(2023)"            # population, years 2023 ->
           "&outputFormat=csv&outputFormatParams=separatorsemicolon")          # CSV with ';' like the manual export
# Sex and age are left out of the query -> SSB sums them ("elimination"), so we get total population.

SOURCES = [                                                                    # the datasets the pipeline handles
    {"source": "elhub", "dataset": "consumption_per_group_municipality_hour",  # hourly kWh per municipality and group
     "mode": "snapshot", "sep": ";", "required": True,                         # .csv export = last ~3 years, no date filter
     "url": ELHUB_API + "/municipalities.csv?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR"},
    {"source": "elhub", "dataset": "norgespris_consumption_per_group_eac_mba", # kWh with/without Norgespris per price area
     "mode": "snapshot", "sep": ",", "required": False,
     "url": ELHUB_API + "/price-areas.csv?dataset=NORGESPRIS_CONSUMPTION_PER_GROUP_EAC_MBA"},
    {"source": "elhub", "dataset": "norgespris_count_per_municipality_consumption_group",  # Norgespris meters per municipality
     "mode": "snapshot", "sep": ",", "required": False,
     "url": "https://data.elhub.no/download/norgespris_count_per_municipality_consumption_group/"
            "norgespris_count_per_municipality_consumption_group-all-en-0000-00-00.csv"},
    {"source": "ssb", "dataset": "population_07459",                          # population per municipality
     "mode": "snapshot", "sep": ";", "required": True, "url": SSB_URL},
    {"source": "met", "dataset": "frost_stations",                            # 1) station -> municipality
     "mode": "snapshot", "sep": None, "required": True, "auth": "frost", "ext": "json",
     "url": FROST_API + "/sources/v0.jsonld?types=SensorSystem&validtime=2023-01-01/2027-01-01"},
    {"source": "met", "dataset": "frost_timeseries",                          # 2) which stations have daily mean temp
     "mode": "snapshot", "sep": None, "required": True, "auth": "frost", "ext": "json",
     "url": FROST_API + "/observations/availableTimeSeries/v0.jsonld?elements=" + FROST_ELEMENT
                      + "&referencetime=2023-11-01/2027-01-01"},
    {"source": "met", "dataset": "frost_observations",                        # 3) daily mean temp per station
     "mode": "frost_observations", "sep": None, "required": True, "auth": "frost", "ext": "json",
     "url": FROST_API + "/observations/v0.jsonld?elements=" + FROST_ELEMENT
                      + "&sources={sources}&referencetime={start}/{end}"},
    {"source": "met", "dataset": "seklima_daily_mean_temperature",            # manual fallback, not needed with Frost
     "mode": "manual", "sep": ";", "required": False},
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
    """First line of a CSV (or of the first CSV inside a ZIP), without BOM and line break; '(json)' for JSON."""
    if path.lower().endswith(".json"):                         # JSON has no header line to compare
        return "(json)"
    if zipfile.is_zipfile(path):                               # ZIP -> look at the first CSV inside
        with zipfile.ZipFile(path) as zf:
            csvs = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))  # CSV members only
            if not csvs:                                       # a ZIP without CSV: nothing to compare
                return ""
            with zf.open(csvs[0]) as member:
                first = member.readline(2000)                  # first line of the first CSV
    else:
        with open(path, "rb") as f:                            # binary mode, so odd characters cannot crash us
            first = f.readline(2000)                           # first line only, max 2000 bytes
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
        if f.read(2) != b"\x1f\x8b":                           # gzip files always start with these two bytes
            return                                             # not gzip -> nothing to do
    with gzip.open(path, "rb") as src, open(path + ".unpacked", "wb") as dst:  # read compressed, write plain
        shutil.copyfileobj(src, dst)                           # stream, so big files are fine
    os.replace(path + ".unpacked", path)                       # swap the unpacked file in
    logger.info(f"  unpacked gzip: {os.path.basename(path)}")


def auth_headers(source_cfg):
    """Extra HTTP headers for sources that need a login. {} = open source, None = login missing."""
    if source_cfg.get("auth") != "frost":                      # Elhub and SSB are open
        return {}
    client_id = os.environ.get("FROST_CLIENT_ID", "").strip()  # from .env or a GitHub secret
    if not client_id or client_id.startswith("your-"):         # missing, or still the placeholder from .env
        return None
    token = base64.b64encode(f"{client_id}:".encode()).decode()  # "<client id>:" = empty password (Frost docs)
    return {"Authorization": f"Basic {token}"}


def download(url, target_path, headers=None, attempts=3):
    """Download url to target_path; retry server/network errors, raise the last error if all attempts fail."""
    all_headers = {"User-Agent": "DAT535-group24-student-project"}  # say politely who we are
    all_headers.update(headers or {})                          # add e.g. the Authorization header
    for attempt in range(1, attempts + 1):                     # attempt 1, 2, 3
        try:
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
            error = e
        logger.warning(f"Download attempt {attempt}/{attempts} failed for {url}: {error}")
        if attempt == attempts:                                # the last attempt also failed ...
            raise error                                        # ... let the caller record the failure
        time.sleep(5 * attempt)                                # wait 5 s, then 10 s


def ingest_file(state, src_path, source, dataset, period, origin, original_filename):
    """Copy one file unchanged into Bronze unless an identical file is already there."""
    checksum = content_sha256(src_path)                         # fingerprint of the content (inside a ZIP: the files)
    header = read_header(src_path)                              # column-names line, or "(json)"
    earlier = [r for r in state["manifest"]                     # earlier files of the SAME dataset ...
               if r["dataset"] == dataset and r["status"] == "ingested"]  # ... that were actually stored

    row = {"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,  # metadata for the manifest
           "period": period, "origin": origin, "original_filename": original_filename,
           "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "size_bytes": os.path.getsize(src_path), "sha256": checksum, "header": header}

    if any(r["sha256"] == checksum for r in earlier):           # exact same content seen before?
        row["status"] = "skipped_duplicate"                     # yes -> do not store it again
        state["counts"]["skipped_duplicate"] += 1
        logger.info(f"  skip (already ingested): {dataset} {period} {original_filename}")
    else:
        if not earlier:                                         # first file ever for this dataset
            row["schema_status"] = "first_file"
        elif earlier[-1]["header"] == header:                   # same columns as the newest earlier file
            row["schema_status"] = "ok"
        else:                                                   # columns changed -> keep the file, but flag it
            row["schema_status"] = "changed"
            logger.warning(f"  HEADER CHANGED for {dataset}: check before Silver!")
        is_zip = zipfile.is_zipfile(src_path)                                 # Elhub exports are ZIPs
        stored_name = original_filename
        if is_zip and not stored_name.lower().endswith(".zip"):               # give it the correct file ending
            stored_name = os.path.splitext(stored_name)[0] + ".zip"
        target_dir = os.path.join(BRONZE_DIR, source, dataset, period)        # e.g. bronze/met/frost_observations/2025-10
        os.makedirs(target_dir, exist_ok=True)                                # create the folder if missing
        target = os.path.join(target_dir, f"{state['run_id']}__{stored_name}")  # run_id keeps versions apart
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
    """Download url to a temp file and ingest it; record every outcome in the manifest."""
    source, dataset = cfg["source"], cfg["dataset"]
    level = "failed" if cfg["required"] else "warning"           # how bad a failure is for this source
    base_row = {"run_id": state["run_id"], "env": ENV, "source": source, "dataset": dataset,
                "period": period, "origin": url, "original_filename": filename,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    headers = auth_headers(cfg)                                  # {} for open sources, None if login is missing
    if headers is None:                                          # Frost without a client ID
        state["counts"][level] += 1
        append_manifest(dict(base_row, status=f"{level}: FROST_CLIENT_ID missing (set it in .env)"))
        logger.error(f"  {level.upper()}: {dataset} needs FROST_CLIENT_ID (in .env or as GitHub secret)")
        return
    tmp_path = os.path.join(state["tmp_dir"], filename)          # temp file in bronze/_tmp/<run_id>/
    try:
        download(url, tmp_path, headers)                         # may raise after failed attempts
        ingest_file(state, tmp_path, source, dataset, period, url, filename)  # checks + copy + manifest
    except urllib.error.HTTPError as e:
        if e.code == 404 and cfg["mode"] == "frost_observations":  # Frost: "no data for these stations/month"
            state["counts"]["no_data"] += 1
            row = dict(base_row, status="no_data")
            append_manifest(row)
            state["manifest"].append(row)                        # remember: do not ask again (unless recent)
            logger.info(f"  no data: {filename}")
        else:
            state["counts"][level] += 1
            append_manifest(dict(base_row, status=f"{level}: HTTP {e.code} {e.reason}"[:200]))
            logger.error(f"  {level.upper()}: {url} (HTTP {e.code} {e.reason})")
    except Exception as e:                                       # network error etc.
        state["counts"][level] += 1
        append_manifest(dict(base_row, status=f"{level}: {type(e).__name__} {e}"[:200]))
        logger.error(f"  {level.upper()}: {url} ({e})")
    finally:
        if os.path.exists(tmp_path):                             # always clean up the temp file
            os.remove(tmp_path)


def frost_station_ids(state):
    """Station ids that have daily mean temperature, from the newest frost_timeseries file in Bronze."""
    rows = [r for r in state["manifest"]                         # stored availableTimeSeries files
            if r["dataset"] == "frost_timeseries" and r["status"] == "ingested"]
    if not rows:                                                 # step 2 did not succeed (yet)
        return []
    path = os.path.join(BRONZE_DIR, rows[-1]["bronze_path"])     # newest version
    with open(path, encoding="utf-8") as f:
        data = json.load(f).get("data", [])                      # Frost puts the records in "data"
    return sorted({d["sourceId"].split(":")[0] for d in data})   # "SN18700:0" -> "SN18700", no duplicates


def frost_observation_requests(state, cfg):
    """List of (url, month, filename): every month x every group of stations, minus what we already have."""
    ids = frost_station_ids(state)                               # stations to ask for
    if not ids:
        logger.error("  no station list from frost_timeseries - cannot request observations")
        return []
    n = FROST_STATIONS_PER_REQUEST
    groups = [ids[i:i + n] for i in range(0, len(ids), n)]       # [[SN..100 ids], [SN..], ...]
    start, end = month_range()                                   # period for dev or prod, from .env
    months = month_list(start, end or last_complete_month())     # None = up to the last complete month
    recent = set(months[-REDOWNLOAD_RECENT_MONTHS:])             # always re-request the newest months
    have = {r["original_filename"] for r in state["manifest"]    # files we already got (or know are empty)
            if r["dataset"] == cfg["dataset"] and r["status"] in ("ingested", "skipped_duplicate", "no_data")}
    requests = []
    for month in months:
        for g, group in enumerate(groups, start=1):
            filename = f"frost_obs_{month}_g{g:02d}.json"         # e.g. frost_obs_2025-10_g03.json
            if filename in have and month not in recent:         # already stored and not recent -> skip
                continue
            url = cfg["url"].format(sources=",".join(group),     # SN18700,SN18950,...
                                    start=f"{month}-01",          # first day of the month
                                    end=f"{next_month(month)}-01")  # first day of next month (end not included)
            requests.append((url, month, filename))
    logger.info(f"  {len(ids)} stations in {len(groups)} groups -> {len(requests)} requests")
    return requests


def run_bronze():
    """Run Bronze ingestion for all sources and return a summary dict."""
    start_time = time.time()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")  # microseconds keep rapid runs apart
    state = {"run_id": run_id,                                 # shared state for the helper functions
             "tmp_dir": os.path.join(BRONZE_DIR, "_tmp", run_id),
             "manifest": read_manifest(),                      # everything ingested in earlier runs
             "counts": {"ingested": 0, "skipped_duplicate": 0, "no_data": 0, "warning": 0, "failed": 0}}
    os.makedirs(state["tmp_dir"], exist_ok=True)
    logger.info(f"Bronze run {run_id} (env={ENV}) -> {BRONZE_DIR}")

    for cfg in SOURCES:                                        # one dataset at a time, in registry order
        source, dataset, mode = cfg["source"], cfg["dataset"], cfg["mode"]
        logger.info(f"[{source}/{dataset}] mode={mode}")
        ext = cfg.get("ext", "csv")                            # file extension (Frost gives JSON)

        if mode == "snapshot":                                 # one file with everything
            download_and_ingest(state, cfg, cfg["url"], "snapshot", f"{dataset}.{ext}")
        elif mode == "frost_observations":                     # many small requests
            if auth_headers(cfg) is None:                      # no login -> one clear failure, not hundreds
                download_and_ingest(state, cfg, cfg["url"], "-", "frost_obs.json")
            else:
                for url, month, filename in frost_observation_requests(state, cfg):
                    download_and_ingest(state, cfg, url, month, filename)

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
