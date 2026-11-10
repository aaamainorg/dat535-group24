# DAT535 Group 24 – Norgespris and electricity use

Batch data pipeline (PySpark, Medallion architecture) for the course **DAT535 Data Engineering** at UiS, autumn 2026.

**Question:** has daily electricity use per inhabitant in each Norwegian municipality changed after Norgespris
started (1 October 2025), taking temperature into account – and where is the change largest?

| Source | Data | How |
|---|---|---|
| Elhub Energy Data API | Hourly consumption per municipality and consumption group; Norgespris consumption | API CSV export, last ~3 years per file (open) |
| SSB table 07459 | Population per municipality, 1 January each year | PxWebApi v2 (open) |
| MET Frost API | Weather stations with municipality, and their daily mean temperature | API (free client ID, in `.env`) |
| MET Seklima (fallback) | Daily mean temperature per weather station | Manual CSV export, only if Frost is down |

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
├── .env.example                           # template for .env (credentials) - copy to .env, never commit .env
├── requirements.txt                       # Python packages
├── run_pipeline.py                        # one entry point: python run_pipeline.py bronze | all
├── bronze_pipeline.py                     # Bronze ingestion (generated from notebook 01, sections 3.1-3.9)
├── notebooks/
│   └── 01_scope_dataset_bronze.ipynb      # sections 1-3 of the project definition, every line explained
├── tests/
│   └── test_bronze.py                     # pytest, no internet or Spark needed
├── scripts/
│   ├── setup_env.sh                       # prepares Python venv + Java (used by every workflow)
│   └── manifest_summary.py                # writes the Bronze run table on the GitHub Actions run page
└── .github/workflows/
    ├── dat535-dev.yml                     # push to dev / manual       -> tests + pipeline into ~/dat535-data/dev
    ├── dat535-prod.yml                    # push to main / manual      -> tests + pipeline into ~/dat535-data/prod (approval)
    └── pipeline.yml                       # monthly schedule / manual  -> pipeline into ~/dat535-data/prod
```

Data is **not** stored in git. It lives on the server:

```text
~/dat535-data/<dev|prod>/
├── landing/<source>/<dataset>/            # put manual exports here (Seklima, ...)
└── bronze/
    ├── _manifest.csv                      # one line per file: source, time, size, SHA-256, status
    └── <source>/<dataset>/<period>/<run_id>__<file>
```

## Setup: GitHub Actions runner on your own machine (once)

All three workflows use `runs-on: self-hosted`, so they run on **your computer** through a GitHub runner.
Data goes to `~/dat535-data/dev` and `~/dat535-data/prod` on that computer – the same folders the notebook uses.

1. **Python and Java on the machine.**
   - Python 3.9+ (`python3 --version`)
   - Java 17+ for PySpark 4 (`java -version`). macOS: `brew install openjdk@17`, then follow brew's message to link it
     (`sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk`).
   - Check that everything works: `bash scripts/setup_env.sh` – it creates `~/spark-env`, installs `requirements.txt`
     and prints the Python, PySpark and Java versions. The workflows run the same script.

2. **Register the runner.** On GitHub: repo → *Settings → Actions → Runners → New self-hosted runner*.
   Choose your OS (macOS, ARM64 for Apple Silicon / x64 for Intel) and run the commands GitHub shows, e.g.:
   ```bash
   mkdir ~/actions-runner && cd ~/actions-runner
   # download + extract: copy the exact curl/tar lines from the GitHub page
   ./config.sh --url https://github.com/aaamainorg/dat535-group24 --token <TOKEN FROM THE PAGE>
   ```
3. **Start the runner.**
   - Simplest: `./run.sh` in a terminal. The runner is online while that terminal is open.
   - Always on in the background: `./svc.sh install && ./svc.sh start` (macOS: no `sudo`).
     A background service does not read your shell profile – if a run cannot find Java, add a line
     `JAVA_HOME=/path/to/java` (output of `/usr/libexec/java_home` on macOS) to `~/actions-runner/.env` and restart the service.

4. **GitHub environments.** Repo → *Settings → Environments*: create `dev` (no rules) and `main`
   (add *Required reviewers* = group members). Prod deployments then wait for an approval click.
   (Required reviewers are free for public repos; for private repos they need a paid plan – then leave the rule out.)

5. **Frost client ID (required).** Free at <https://frost.met.no/auth/requestCredentials.html>.
   - **Locally (notebook / terminal):** put it in `.env` in the repo root – see *Credentials* below.
   - **GitHub Actions:** add it as repository secret `FROST_CLIENT_ID` (*Settings → Secrets and variables → Actions*).

6. **Manual files (only as fallback).** Any dataset can be fed by hand via `~/dat535-data/<dev|prod>/landing/<source>/<dataset>/`,
   e.g. a Seklima export in `landing/met/seklima_daily_mean_temperature/` if Frost is down.

**Security:** a self-hosted runner executes whatever the workflow says on your computer. That is why the workflows
have no `pull_request` trigger – keep it that way while the repo is public.

**Laptop asleep?** A scheduled run (`pipeline.yml`) only starts when the runner is online. If it was missed,
start it by hand: *Actions → Pipeline → Run workflow*.

### Which workflow does what

| Workflow | Trigger | Environment | Data folder |
|---|---|---|---|
| `dat535-dev.yml` | push to `dev`, manual | `dev` | `~/dat535-data/dev` |
| `dat535-prod.yml` | push to `main` (after PR), manual with stage choice | `main` (approval) | `~/dat535-data/prod` |
| `pipeline.yml` | monthly (5th, 06:00 UTC), manual | – | `~/dat535-data/prod` |

Each run shows the Bronze manifest as a table on the run page (prod) and uploads `_manifest.csv` as an artifact.
Prod runs never overlap (`concurrency` group), because they write to the same folder.

## Credentials and settings (`.env`)

```bash
cp .env.example .env      # once, in the repo root
# then edit .env:
# FROST_CLIENT_ID=<your client id>
# FROST_CLIENT_SECRET=<your client secret>
```

- `.env` is in `.gitignore`: **never commit it** and never paste the values into a notebook or a chat. `.env.example` (with placeholders) is committed so everyone knows which keys are needed.
- The pipeline reads `.env` from the repo root (also when the notebook runs from `notebooks/`). Values already set in the environment win, so GitHub secrets override `.env`.
- Frost uses **only the client ID** (as user name, empty password) for open data. The secret is for OAuth2/restricted data; we keep it in `.env` but do not send it.
- **Period:** `.env` also holds which months of Frost data to fetch – `NORGESPRIS_DEV_START_MONTH` / `_END_MONTH` and
  `NORGESPRIS_PROD_START_MONTH` / `_END_MONTH` (format `YYYY-MM`, empty end = last complete month). The pipeline uses the
  pair that matches `NORGESPRIS_ENV`. In GitHub Actions set them as repository **variables** (not secrets); if you don't,
  the workflows use the same defaults as `.env.example`.
- If a value leaks (e.g. committed by mistake): create a new client at frost.met.no and delete the old one – removing the file from git is not enough.

## Running

```bash
source ~/spark-env/bin/activate

python run_pipeline.py bronze            # dev by default -> ~/dat535-data/dev
NORGESPRIS_ENV=prod NORGESPRIS_OUTPUT_DIR=~/dat535-data/prod python run_pipeline.py bronze   # by hand in prod

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

Elhub open data (NLOD) · Statistics Norway, table 07459 (CC BY 4.0) · Norwegian Meteorological Institute, Frost/Seklima (CC BY 4.0).
