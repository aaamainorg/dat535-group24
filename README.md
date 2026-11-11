# DAT535 Group 24 – Norgespris and electricity use

Batch data pipeline (PySpark, Medallion architecture, **PostgreSQL** as target) for the course **DAT535 Data Engineering** at UiS, autumn 2026.

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
| Scope, dataset, landing (raw files) | ✅ done | `notebooks/01_bronze.ipynb` sections 1–4, `bronze_pipeline.py` |
| Bronze (PostgreSQL schema `bronze`) | ✅ done | notebook 01 section 5, `bronze_to_postgres.py` |
| Silver | ⏳ next | – |
| Gold + dashboard | ⏳ | – |

## Repository structure

```text
.
├── README.md
├── env.example                            # template for .env (credentials + settings) - copy to .env, never commit .env
├── requirements.txt                       # Python packages
├── run_pipeline.py                        # one entry point: python run_pipeline.py bronze | postgres | all
├── bronze_pipeline.py                     # stage "bronze":   sources -> landing files (generated from notebook 01, 3.1-3.9)
├── bronze_to_postgres.py                  # stage "postgres": landing files -> PostgreSQL schema bronze (notebook 01, 4.1/4.2/4.5/5)
├── notebooks/
│   └── 01_bronze.ipynb                    # sections 1-3 of the project definition + load into PostgreSQL, every line explained
├── tests/
│   ├── test_bronze.py                     # pytest, no internet or Spark needed
│   └── test_postgres.py                   # pytest for the PostgreSQL settings (no database needed)
├── scripts/
│   ├── setup_env.sh                       # prepares Python venv + Java (used by every workflow)
│   ├── init_postgres.sql                  # one-time PostgreSQL setup: role, databases, schemas
│   └── manifest_summary.py                # writes the Bronze run table on the GitHub Actions run page
└── .github/workflows/
    ├── norgespris-dev.yml                 # push to dev / manual       -> tests + pipeline (dev)
    ├── norgespris-prod.yml                # push to main / manual      -> tests + pipeline (prod, approval)
    └── pipeline.yml                       # monthly schedule / manual  -> pipeline (prod)
```

Data is **not** stored in git. The data flow on the machine that runs the pipeline:

```text
sources (Elhub, SSB, Frost)
   │  stage "bronze"   (python run_pipeline.py bronze)
   ▼
/tmp/norgespris-landing/<dev|prod>/        # LANDING: raw files, exactly as delivered
├── _manifest.csv                          # one line per file: source, time, size, SHA-256, status
├── _incoming/<run_id>/                    # downloads arrive here; deleted once stored
├── _manual/<source>/<dataset>/            # drop manual exports here (e.g. a Seklima CSV)
└── <source>/<dataset>/<period>/<run_id>__<file>
    e.g. elhub/consumption_per_group_municipality_hour/snapshot/<run_id>__consumption_per_group_municipality_hour.zip
   │  stage "postgres" (python run_pipeline.py postgres)
   ▼
PostgreSQL norgespris_<dev|prod>
├── bronze.<dataset>                       # BRONZE: one table per dataset, all text + _source_file, _landing_run_id, _loaded_at
├── bronze.manifest, bronze._load_log      # monitoring: what was ingested / loaded when
├── silver.*                               # next notebook
└── gold.*                                 # later
```

Why the landing folder is in `/tmp`: raw files are short-lived input – after the `postgres` stage the data is in the
`bronze` tables. `/tmp` is cleaned on reboot (macOS also removes files not used for a few days); the next run then simply
downloads again. Want to keep the raw files? Set `NORGESPRIS_LANDING_ROOT` in `.env` to a folder in your home directory.

## Setup: PostgreSQL (once)

The pipeline writes to a PostgreSQL server **on the same machine** (the one that runs the notebook and the GitHub runner).
Any version from 14 up works; the instructions use 16.

### 1. Install and start PostgreSQL

**macOS (Homebrew)**
```bash
brew install postgresql@16
brew services start postgresql@16                 # starts now and at every login
echo 'export PATH="$(brew --prefix)/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc   # psql on the PATH
psql -d postgres -c "SELECT version();"           # check: your macOS user is the superuser
```

**Ubuntu / Debian (e.g. a course VM)**
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo systemctl enable --now postgresql            # starts now and at every boot
sudo -u postgres psql -c "SELECT version();"      # check: the superuser is the Linux user "postgres"
```

**Docker (any OS, if you prefer not to install it)**
```bash
docker run -d --name norgespris-postgres --restart unless-stopped \
  -e POSTGRES_PASSWORD=choose-an-admin-password -p 5432:5432 \
  -v norgespris-pgdata:/var/lib/postgresql/data postgres:16
```
The named volume `norgespris-pgdata` keeps the data when the container is removed.

### 2. Create the role, databases and schemas

From the repo root, run `scripts/init_postgres.sql` **once** as superuser. It creates the login role `norgespris`,
the databases `norgespris_dev` and `norgespris_prod`, and the schemas `bronze`, `silver`, `gold` in both.
Choose a password for `norgespris` (not the admin password):

```bash
# macOS (Homebrew)
psql -d postgres -v pw='choose-a-password' -f scripts/init_postgres.sql
# Ubuntu / Debian
sudo -u postgres psql -v pw='choose-a-password' < scripts/init_postgres.sql
# Docker
docker exec -i norgespris-postgres psql -U postgres -v pw='choose-a-password' < scripts/init_postgres.sql
```

Check that the pipeline user can log in:
```bash
psql -h localhost -U norgespris -d norgespris_dev -c '\dn'     # lists bronze, gold, silver
```

Start over (deletes everything): `DROP DATABASE norgespris_dev; DROP DATABASE norgespris_prod; DROP ROLE norgespris;` as superuser, then run the script again.

### 3. Tell the pipeline how to connect

In `.env` (see *Credentials and settings* below):
```bash
NORGESPRIS_PG_HOST=localhost
NORGESPRIS_PG_PORT=5432
NORGESPRIS_PG_USER=norgespris
NORGESPRIS_PG_PASSWORD=choose-a-password      # the password from step 2
```
The database is picked automatically: `norgespris_dev` in dev, `norgespris_prod` in prod.
For GitHub Actions add the password as repository **secret** `NORGESPRIS_PG_PASSWORD`
(host/port/user default to the values above; override them as repository variables if needed).

### 4. JDBC driver

Spark talks to PostgreSQL through the PostgreSQL JDBC driver. It is downloaded automatically from Maven Central the first
time Spark starts (`org.postgresql:postgresql:42.7.4`, cached in `~/.ivy2`). Without access to Maven Central, download
`postgresql-42.7.4.jar` from <https://jdbc.postgresql.org/download/> and set `NORGESPRIS_PG_JDBC_JAR=/full/path/postgresql-42.7.4.jar` in `.env`.

### 5. Look at the data

```bash
psql -h localhost -U norgespris -d norgespris_dev
\dt bronze.*                                                   -- tables in the bronze schema
SELECT dataset, rows, loaded_at FROM bronze._load_log ORDER BY loaded_at DESC;
SELECT * FROM bronze.population_07459 LIMIT 5;
```
GUI alternatives: pgAdmin, DBeaver, or the PostgreSQL extension in VS Code (host `localhost`, user `norgespris`).

## Setup: GitHub Actions runner on your own machine (once)

All three workflows use `runs-on: self-hosted`, so they run on **your computer** through a GitHub runner.
Raw files go to `/tmp/norgespris-landing/dev` and `/prod` on that computer and tables to `norgespris_dev` / `norgespris_prod` – the same places the notebook uses.

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

5. **Secrets (required).** *Settings → Secrets and variables → Actions → Secrets*:
   - `FROST_CLIENT_ID` – free at <https://frost.met.no/auth/requestCredentials.html>,
   - `NORGESPRIS_PG_PASSWORD` – the PostgreSQL password from *Setup: PostgreSQL*.
   Locally (notebook / terminal) the same values go in `.env` – see *Credentials and settings* below.
   PostgreSQL must be running on the runner machine when a workflow runs.

6. **Manual files (only as fallback).** Any dataset can be fed by hand via `/tmp/norgespris-landing/<dev|prod>/_manual/<source>/<dataset>/`,
   e.g. a Seklima export in `_manual/met/seklima_daily_mean_temperature/` if Frost is down.

**Security:** a self-hosted runner executes whatever the workflow says on your computer. That is why the workflows
have no `pull_request` trigger – keep it that way while the repo is public.

**Laptop asleep?** A scheduled run (`pipeline.yml`) only starts when the runner is online. If it was missed,
start it by hand: *Actions → Pipeline → Run workflow*.

### Which workflow does what

| Workflow | Trigger | Environment | Landing folder | Database |
|---|---|---|---|---|
| `norgespris-dev.yml` | push to `dev`, manual | `dev` | `/tmp/norgespris-landing/dev` | `norgespris_dev` |
| `norgespris-prod.yml` | push to `main` (after PR), manual with stage choice | `main` (approval) | `/tmp/norgespris-landing/prod` | `norgespris_prod` |
| `pipeline.yml` | monthly (5th, 06:00 UTC), manual | – | `/tmp/norgespris-landing/prod` | `norgespris_prod` |

Every workflow runs `python run_pipeline.py all` = stage `bronze` (sources → landing files) and then stage `postgres` (landing → `bronze` tables).

Each run shows the landing manifest as a table on the run page (prod) and uploads `_manifest.csv` as an artifact.
Prod runs never overlap (`concurrency` group), because they write to the same folder and database.

## Credentials and settings (`.env`)

```bash
cp env.example .env       # once, in the repo root
# then edit .env:
# FROST_CLIENT_ID=<your client id>
# FROST_CLIENT_SECRET=<your client secret>
# NORGESPRIS_PG_PASSWORD=<the password from init_postgres.sql>
```

- `.env` is in `.gitignore`: **never commit it** and never paste the values into a notebook or a chat. `env.example` (with placeholders) is committed so everyone knows which keys are needed.
- The pipeline reads `.env` from the repo root (also when the notebook runs from `notebooks/`). Values already set in the environment win, so GitHub secrets override `.env`.
- Frost uses **only the client ID** (as user name, empty password) for open data. The secret is for OAuth2/restricted data; we keep it in `.env` but do not send it.
- **Period:** `.env` also holds which months of Frost data to fetch – `NORGESPRIS_DEV_START_MONTH` / `_END_MONTH` and
  `NORGESPRIS_PROD_START_MONTH` / `_END_MONTH` (format `YYYY-MM`, empty end = last complete month). The pipeline uses the
  pair that matches `NORGESPRIS_ENV`. In GitHub Actions set them as repository **variables** (not secrets); if you don't,
  the workflows use their own defaults (see the `env:` block in each workflow file).
- **Landing folder:** `NORGESPRIS_LANDING_ROOT` (default `/tmp/norgespris-landing`; `dev/` or `prod/` is added).
- **PostgreSQL:** `NORGESPRIS_PG_HOST`, `_PORT`, `_USER`, `_PASSWORD` (and optional `_DATABASE`, `_JDBC_JAR`) – see *Setup: PostgreSQL*.
- If a value leaks (e.g. committed by mistake): create a new Frost client and delete the old one, and change the
  PostgreSQL password (`ALTER ROLE norgespris PASSWORD '...';`) – removing the file from git is not enough.

## Running

```bash
source ~/spark-env/bin/activate

python run_pipeline.py all               # dev by default: sources -> /tmp/norgespris-landing/dev -> norgespris_dev.bronze
python run_pipeline.py bronze            # only download/copy raw files into the landing folder
python run_pipeline.py postgres          # only (re)load the landing files into the bronze schema
NORGESPRIS_ENV=prod python run_pipeline.py all   # by hand in prod (/tmp/norgespris-landing/prod, norgespris_prod)

pytest -q tests/                         # unit tests
jupyter notebook notebooks/              # the notebooks
```

Re-running is safe: files already in the landing folder (same checksum) are skipped, and a `bronze` table is only rewritten when its landing files changed.

## Branches and deployment

```bash
git checkout -b dev                      # day-to-day work
git add . && git commit -m "feat(bronze): ..."
git push -u origin dev                   # -> dev workflow runs on the server
```

Release: open a pull request `dev → main`, get it reviewed, merge → prod workflow runs after approval.
Check runs under the **Actions** tab; each run uploads the Bronze manifest as an artifact.

## Keeping the notebook and the script in sync

`bronze_pipeline.py` contains exactly the code cells of notebook 01, sections 3.1–3.9, and `bronze_to_postgres.py`
the cells of 4.1 (settings), 4.2/4.5 (readers) and 5.1–5.2 (loader).
When you change the logic, change the notebook cell **and** paste it into the matching `.py` file, then run `pytest`.

## Licences and credits

Elhub open data (NLOD) · Statistics Norway, table 07459 (CC BY 4.0) · Norwegian Meteorological Institute, Frost/Seklima (CC BY 4.0).
