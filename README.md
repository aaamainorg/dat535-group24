# DAT535 Group 24 – Norgespris and electricity use

Batch data pipeline (PySpark, Medallion architecture) for the course **DAT535 Data Engineering** at UiS, autumn 2026.

**Question:** has daily electricity use per inhabitant in each Norwegian municipality changed after Norgespris
started (1 October 2025), taking temperature into account – and where is the change largest?

| Source | Data | How |
|---|---|---|
| Elhub Energy Data API | Hourly consumption per municipality and consumption group; Norgespris consumption | API CSV export, last ~3 years per file (open) |
| SSB table 07459 | Population per municipality, 1 January each year | PxWebApi v2 (open) |
| MET Seklima | Daily mean temperature per weather station | Manual CSV export |
| MET Frost API (optional) | Weather station → municipality | API (free client ID) |

## Status

| Layer | Status | Where |
|---|---|---|
| Scope, dataset, Bronze | ✅ done | `notebooks/01_scope_dataset_bronze.ipynb`, `bronze_pipeline.py` |
| Silver | ⏳ next | – |
| Gold + dashboard | ⏳ | – |

## Repository structure

```text
.
├── README.md
├── requirements.txt                       # Python packages
├── run_pipeline.py                        # one entry point: python run_pipeline.py bronze | all
├── bronze_pipeline.py                     # Bronze ingestion (generated from notebook 01, sections 3.1-3.9)
├── notebooks/
│   └── 01_scope_dataset_bronze.ipynb      # sections 1-3 of the project definition, every line explained
├── tests/
│   └── test_bronze.py                     # pytest, no internet or Spark needed
└── .github/workflows/
    ├── dat535-dev.yml                     # push to dev  -> tests + pipeline
    └── dat535-prod.yml                    # push to main / monthly / manual -> tests + pipeline (full period)
```

Data is **not** stored in git. It lives on the server:

```text
~/dat535-data/<dev|prod>/
├── landing/<source>/<dataset>/            # put manual exports here (Seklima, ...)
└── bronze/
    ├── _manifest.csv                      # one line per file: source, time, size, SHA-256, status
    └── <source>/<dataset>/<period>/<run_id>__<file>
```

## Setup (once, on the course server)

1. **Spark and Python environment.** The workflows expect `~/spark-env` (Python venv), `/opt/spark` and Java 8
   at `/usr/lib/jvm/java-8-openjdk-amd64` – the same setup as in the course labs. If `~/spark-env` is missing:
   ```bash
   python3 -m venv ~/spark-env
   source ~/spark-env/bin/activate
   pip install -r requirements.txt
   ```
   If the paths differ on your server, change `SPARK_HOME` / `JAVA_HOME` in both workflow files.
   Version check: PySpark 4.x needs Java 17+, PySpark 3.5 works with Java 8. Keep the `pyspark` version in
   `~/spark-env` the same as the Spark in `/opt/spark` (`pip install "pyspark==<that version>"`).

2. **Self-hosted GitHub runner.** Repo → *Settings → Actions → Runners → New self-hosted runner* (Linux x64),
   run the commands GitHub shows on the server, then `sudo ./svc.sh install && sudo ./svc.sh start`.
   The runner needs internet access to `api.elhub.no`, `data.elhub.no`, `data.ssb.no` and `frost.met.no`.

3. **GitHub environments.** Repo → *Settings → Environments*:
   - `dev` – no protection rules.
   - `main` – add *Required reviewers* (group members). Prod runs then wait for an approval click,
     **including the monthly scheduled run**.

4. **Frost client ID (optional, recommended).** Register for free at <https://frost.met.no/auth/requestCredentials.html>
   and add the client ID as repository secret `FROST_CLIENT_ID` (*Settings → Secrets and variables → Actions*).
   Without it the station → municipality download is skipped.

5. **Manual files.** Export Seklima (all stations, *Middeltemperatur (døgn)*, from 2023-11-01, CSV) and put the file(s) in
   `~/dat535-data/dev/landing/met/seklima_daily_mean_temperature/` (and the same under `prod/`).
   Several files are fine, e.g. one per year.

## Running

```bash
source ~/spark-env/bin/activate

python run_pipeline.py bronze            # dev by default -> ~/dat535-data/dev
DAT535_ENV=prod DAT535_OUTPUT_DIR=~/dat535-data/prod python run_pipeline.py bronze   # by hand in prod

pytest -q tests/                         # unit tests
jupyter notebook notebooks/              # the notebooks
```

Re-running is safe: files already in Bronze (same checksum) are skipped.

## Branches and deployment

```bash
git checkout -b dev                      # day-to-day work
git add . && git commit -m "feat(bronze): ..."
git push -u origin dev                   # -> dev workflow runs on the server
```

Release: open a pull request `dev → main`, get it reviewed, merge → prod workflow runs after approval.
Check runs under the **Actions** tab; each run uploads the Bronze manifest as an artifact.

## Keeping the notebook and the script in sync

`bronze_pipeline.py` contains exactly the code cells of notebook 01, sections 3.1–3.9.
When you change ingestion logic, change the notebook cell **and** paste it into `bronze_pipeline.py`, then run `pytest`.

## Licences and credits

Elhub open data (NLOD) · Statistics Norway, table 07459 (CC BY 4.0) · Norwegian Meteorological Institute, Seklima/Frost (CC BY 4.0).
