# Setting up a new environment

Step by step from an empty machine to a working pipeline, notebook and GitHub runner. Works on **macOS** and **Ubuntu/Debian**
(e.g. the course VM). Takes about 30–60 minutes the first time.
What the system does: [architecture.md](architecture.md) · how it runs: [pipelines.md](pipelines.md).

## Contents
1. [What you need](#1-what-you-need)
2. [Get the code](#2-get-the-code)
3. [System packages: Python, Java, PostgreSQL](#3-system-packages-python-java-postgresql)
4. [Python environment](#4-python-environment)
5. [PostgreSQL: role, databases, schemas](#5-postgresql-role-databases-schemas)
6. [Configuration: `.env`](#6-configuration-env)
7. [Check that everything works](#7-check-that-everything-works)
8. [Notebooks (Jupyter or VS Code)](#8-notebooks-jupyter-or-vs-code)
9. [GitHub Actions runner](#9-github-actions-runner)
10. [Troubleshooting](#10-troubleshooting)
11. [Start over](#11-start-over)

---

## 1. What you need

| Item | Requirement |
|---|---|
| Machine | macOS (Intel or Apple Silicon) or Ubuntu 22.04/24.04; ≥ 8 GB RAM (with less, see `NORGESPRIS_SPARK_DRIVER_MEMORY`), ≥ 10 GB free disk |
| Software | Python ≥ 3.9 (3.12 tested), **Java 17+** (PySpark 4), PostgreSQL ≥ 14 (16 tested), git |
| Accounts | GitHub access to the repo; a free **Frost client ID** from <https://frost.met.no/auth/requestCredentials.html> |
| Network | HTTPS to `api.elhub.no`, `data.elhub.no`, `data.ssb.no`, `frost.met.no`, `pypi.org`, `repo1.maven.org` (JDBC driver) |

## 2. Get the code

```bash
cd ~                                                   # a path WITHOUT spaces (important for the GitHub runner)
git clone https://github.com/aaamainorg/dat535-group24.git
cd dat535-group24
```

## 3. System packages: Python, Java, PostgreSQL

**macOS (Homebrew)**
```bash
brew install python@3.12 openjdk@17 postgresql@16
sudo ln -sfn "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-17.jdk
brew services start postgresql@16                      # starts now and at every login
echo 'export PATH="$(brew --prefix)/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc && source ~/.zshrc
```

**Ubuntu / Debian**
```bash
sudo apt update
sudo apt install -y python3-venv python3-pip openjdk-17-jdk postgresql postgresql-contrib
sudo systemctl enable --now postgresql                 # starts now and at every boot
```

**PostgreSQL in Docker instead (any OS)**
```bash
docker run -d --name norgespris-postgres --restart unless-stopped \
  -e POSTGRES_PASSWORD=choose-an-admin-password -p 5432:5432 \
  -v norgespris-pgdata:/var/lib/postgresql/data postgres:16
```

Check: `python3 --version`, `java -version` (17 or newer), `psql --version`.

## 4. Python environment

```bash
bash scripts/setup_env.sh                              # once; safe to run again
source ~/spark-env/bin/activate                        # in every new terminal
```

`setup_env.sh` creates the virtual environment `~/spark-env` (change with `NORGESPRIS_VENV`), installs `requirements.txt` (PySpark,
pandas, pyarrow, pytest, jupyter, matplotlib, ruff), finds Java and prints the versions. On Ubuntu it installs `python3-venv` / Java 17
**only if they are missing**, and only without a password prompt (`sudo -n`); otherwise it prints the `apt` command to run once by hand.
The GitHub workflows run the same script.

## 5. PostgreSQL: role, databases, schemas

Run `scripts/init_postgres.sql` **once** as the PostgreSQL superuser. It creates the login role `norgespris`, the databases
`norgespris_dev` and `norgespris_prod`, and the schemas `bronze`, `silver`, `gold` in both (the Gold stage creates `gold_stage` and
`gold_previous` itself). Choose a password for `norgespris`:

```bash
# macOS (Homebrew): your macOS user is the superuser
psql -d postgres -v pw='choose-a-password' -f scripts/init_postgres.sql
# Ubuntu / Debian: the superuser is the Linux user "postgres"
sudo -u postgres psql -v pw='choose-a-password' < scripts/init_postgres.sql
# Docker
docker exec -i norgespris-postgres psql -U postgres -v pw='choose-a-password' < scripts/init_postgres.sql
```

Check the login: `psql -h localhost -U norgespris -d norgespris_dev -c '\dn'` → lists `bronze`, `gold`, `silver`.

**JDBC driver:** Spark downloads `org.postgresql:postgresql:42.7.4` from Maven Central the first time it starts (cached in `~/.ivy2*`).
Without internet access to Maven: download `postgresql-42.7.4.jar` from <https://jdbc.postgresql.org/download/> and set
`NORGESPRIS_PG_JDBC_JAR` (below).

## 6. Configuration: `.env`

```bash
cp env.example .env                                    # then edit .env
```

`.env` is in `.gitignore` – **never commit it** and never paste its values into a notebook or chat. The code reads it from the repo root
(also when a notebook runs from `notebooks/`); variables already set in the environment win, so GitHub secrets override `.env`.

| Variable | Required | Example / default | Meaning |
|---|---|---|---|
| `NORGESPRIS_ENV` | no | `dev` (default) / `prod` | environment → landing folder and database |
| `FROST_CLIENT_ID` | **yes** | `xxxxxxxx-…` | Frost user name (password empty) |
| `FROST_CLIENT_SECRET` | no | | kept for reference; not sent by the pipeline |
| `NORGESPRIS_DEV_START_MONTH` / `_END_MONTH` | **yes** | `2024-01` / *(empty)* | dev period (`YYYY-MM`, empty end = last complete month) – for Frost **and** Silver |
| `NORGESPRIS_PROD_START_MONTH` / `_END_MONTH` | **yes** | `2024-01` / *(empty)* | prod period |
| `NORGESPRIS_LOAD_MODE` | no | `delta` (default) / `full` | delta = after the first full load only the newest month of the time series; full = whole files every run |
| `NORGESPRIS_DELTA_MONTHS` | no | `1` | delta: how many of the newest months are always (re)loaded |
| `NORGESPRIS_LANDING_ROOT` | no | `/tmp/norgespris-landing` | raw files; `dev/` or `prod/` is added |
| `NORGESPRIS_PG_HOST`, `_PORT`, `_USER` | no | `localhost`, `5432`, `norgespris` | PostgreSQL connection |
| `NORGESPRIS_PG_PASSWORD` | **yes** | | the password from step 5 |
| `NORGESPRIS_PG_DATABASE` | no | `norgespris_<env>` | override the database name |
| `NORGESPRIS_PG_JDBC_JAR` | no | `/path/postgresql-42.7.4.jar` | local JDBC driver instead of Maven |
| `NORGESPRIS_SPARK_DRIVER_MEMORY` | no | `2g` (`1g` with ≤ 8 GB RAM) | Java heap for Spark; Spark uses about this + 0.5 GB RAM |
| `JAVA_HOME` | no | `/usr/lib/jvm/java-17-openjdk-amd64` | only if a notebook kernel cannot find Java – use *your* machine's path |

**If a value leaks** (e.g. committed by mistake): create a new Frost client and delete the old one, and change the PostgreSQL password
(`ALTER ROLE norgespris PASSWORD '…';`) – removing the file from git history is not enough.

**Why `/tmp` for the landing folder:** raw files are short-lived input – after the `postgres` stage the data is in Bronze. `/tmp` is cleaned on
reboot (macOS also removes files not used for a few days); the next run simply downloads again. To keep the raw files, set
`NORGESPRIS_LANDING_ROOT` to a folder in your home directory.

> **Gold needs data before and after 1 October 2025.** Use a start month of `2025-03` or earlier (we use `2024-01`), otherwise Gold
> runs but marks every result as "not reliable" (or stops the dashboard with an explanation).

## 7. Check that everything works

```bash
source ~/spark-env/bin/activate
ruff check --select E9,F63,F7,F82 .                    # lint: no syntax errors / undefined names
pytest -q tests/                                       # 55 passed (local Spark, no database or internet)
python run_pipeline.py --help                          # stages: bronze, postgres, silver, gold, all
python run_pipeline.py all                             # first full run in dev (downloads ~3 years: allow 15-30 min)
```

Then look at the result:
```bash
psql -h localhost -U norgespris -d norgespris_dev \
  -c "SELECT status, first_day, last_day, failed_checks FROM gold.publish_log ORDER BY logged_at DESC LIMIT 1;"
```
`published` = the whole chain works. See [data-quality.md §6](data-quality.md#6-where-to-look--useful-queries) for more queries.

## 8. Notebooks (Jupyter or VS Code)

```bash
source ~/spark-env/bin/activate
jupyter notebook notebooks/                            # or open the folder in VS Code
```

* **Kernel:** choose the Python of `~/spark-env` (VS Code: *Select Kernel → Python Environments → spark-env*). On a remote machine use VS Code
  *Remote-SSH* and pick the kernel on the remote side.
* **Order:** `01_bronze` → `02_silver` → `03_gold` (each reads what the one before wrote to PostgreSQL).
* **One Spark at a time:** every open notebook kernel keeps its own Spark Java process (1.5–2.5 GB RAM). Run the last cell (`spark.stop()`)
  or shut the kernel down before you start the next notebook.
* The first code cell of notebooks 02 and 03 removes `SPARK_HOME` for that kernel, so the PySpark installed with pip is used even if the
  machine has a system Spark (see Troubleshooting).

## 9. GitHub Actions runner

All workflows use `runs-on: self-hosted`: they run on **your** machine, where PostgreSQL and the landing folder are.

1. **Register the runner:** repo → *Settings → Actions → Runners → New self-hosted runner*; choose the OS and run the commands shown, in a
   folder **without spaces**:
   ```bash
   mkdir ~/actions-runner && cd ~/actions-runner
   # download + extract: copy the exact curl/tar lines from the GitHub page
   ./config.sh --url https://github.com/aaamainorg/dat535-group24 --token <TOKEN FROM THE PAGE>
   ./run.sh                                            # runner online while this terminal is open
   ```
   Always on: `./svc.sh install && ./svc.sh start` (Linux: `sudo ./svc.sh install`). A service does not read your shell profile: if Java is
   not found, add `JAVA_HOME=…` to `~/actions-runner/.env` and restart the service.
2. **Environments:** *Settings → Environments*: create `dev` (no rules) and `main` (add *Required reviewers* – prod runs then wait for approval).
3. **Secrets** (*Settings → Secrets and variables → Actions → Secrets*): `FROST_CLIENT_ID`, `NORGESPRIS_PG_PASSWORD`.
4. **Variables** (same page, *Variables* tab) – optional, the workflows have defaults:

   | Variable | Default in the workflows |
   |---|---|
   | `NORGESPRIS_DEV_START_MONTH` / `_END_MONTH` | `2026-10` / `2026-10` (set these, e.g. `2025-01` / empty, for meaningful Gold in dev) |
   | `NORGESPRIS_PROD_START_MONTH` / `_END_MONTH` | `2024-01` / empty |
   | `NORGESPRIS_LOAD_MODE` | `delta` |
   | `NORGESPRIS_PG_HOST`, `_PORT`, `_USER` | `localhost`, `5432`, `norgespris` |
   | `NORGESPRIS_SPARK_DRIVER_MEMORY` | `2g` |

5. **Security:** keep the workflows free of `pull_request` triggers while the repo is public – a fork could otherwise run code on your machine.

How the workflows run: [pipelines.md §5](pipelines.md#5-github-actions-workflows).

## 10. Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `JAVA_HOME is not set` / `Java gateway process exited` in a notebook | the kernel does not see Java | set `JAVA_HOME` in `.env` (your path: macOS `/usr/libexec/java_home`, Ubuntu `dirname $(dirname $(readlink -f $(which java)))`) |
| PySpark version errors, `py4j` mismatch | a system Spark (`SPARK_HOME`, e.g. `/opt/spark`) is used instead of the pip PySpark | `unset SPARK_HOME` (terminal) – notebooks 02/03 already do this for their kernel; `setup_env.sh` warns about it |
| `Out of memory: Killed process … (java)` in `dmesg` | several Spark processes at once, or too little RAM | one notebook with Spark at a time (`spark.stop()`); `NORGESPRIS_SPARK_DRIVER_MEMORY=1g`; check `ps -eo pid,rss,comm --sort=-rss \| head` |
| `Connection refused` to `localhost:5432` | PostgreSQL not running | `brew services start postgresql@16` / `sudo systemctl start postgresql` |
| `password authentication failed for user "norgespris"` | wrong password in `.env` / secret | `ALTER ROLE norgespris PASSWORD '…';` as superuser, update `.env` and the secret |
| Spark cannot download the JDBC driver | no access to Maven Central | `NORGESPRIS_PG_JDBC_JAR=/full/path/postgresql-42.7.4.jar` |
| `Can't assign requested address` (Spark on a Mac runner service) | network binding | the workflows set `SPARK_LOCAL_IP=127.0.0.1`; do the same in a terminal |
| Runner: `/bin/bash: /Users/x/My Folder: No such file or directory` | runner installed in a path with a space | reinstall the runner in `~/actions-runner` |
| `git push` rejected: *refusing to allow an OAuth App to create or update workflow* | your token lacks the `workflow` scope | `gh auth refresh -s workflow`, or push with SSH / a personal access token with `workflow` scope |
| `.github` folder missing on GitHub | hidden folder not added | `git add .github && git commit && git push` |
| Frost `HTTP 401/403` | `FROST_CLIENT_ID` missing/wrong | set it in `.env` / the secret |
| Notebook 03 stops: *no usable days BEFORE 1 October 2025* | the period starts too late | start month ≤ `2025-03`, run `python run_pipeline.py all` again |
| Spark warning *Truncated the string representation of a plan* | very wide tables | harmless; `spark.sql.debug.maxToStringFields=200` is already set |

## 11. Start over

**Only the dev data** (keeps the role, databases and code; the next run downloads and rebuilds everything):
```bash
rm -rf /tmp/norgespris-landing/dev
psql -h localhost -U norgespris -d norgespris_dev -c "DROP SCHEMA IF EXISTS bronze, silver, gold, gold_stage, gold_previous CASCADE;
  CREATE SCHEMA bronze; CREATE SCHEMA silver; CREATE SCHEMA gold;"
```

**Everything** (as PostgreSQL superuser), then step 5 again:
```sql
DROP DATABASE norgespris_dev; DROP DATABASE norgespris_prod; DROP ROLE norgespris;
```

**Fresh Python environment:** `rm -rf ~/spark-env && bash scripts/setup_env.sh`.
