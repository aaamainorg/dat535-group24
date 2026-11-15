#!/usr/bin/env bash
# =============================================================================
# Prepare Python + Java for the pipeline on THIS machine.
# Used by every workflow (first step after checkout), and you can run it by hand:
#     bash scripts/setup_env.sh
#
# What it does:
#   1. creates a Python virtual environment if it does not exist yet
#      (default ~/spark-env, change with NORGESPRIS_VENV)
#   2. installs requirements.txt into it (fast when already installed)
#   3. finds Java (PySpark needs it) and checks it is there
#   4. inside GitHub Actions: makes the venv and JAVA_HOME available to the next steps
# =============================================================================
set -euo pipefail                                   # stop on the first error, on unset variables and in pipes

VENV="${NORGESPRIS_VENV:-$HOME/spark-env}"              # where the virtual environment lives

# --- 1. Python virtual environment -------------------------------------------
if [ ! -x "$VENV/bin/python" ]; then                # no venv yet -> create it
  echo "Creating virtual environment in $VENV"
  python3 -m venv "$VENV"                           # needs python3 (3.9+) on the machine
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"                         # use the venv's python and pip below

sudo apt install -y python3.12-venv python3-pip
# --- 2. Python packages --------------------------------------------------------
pip install --quiet --upgrade pip                   # newer pip resolves packages better
pip install --quiet -r requirements.txt             # no-op when everything is already installed

# --- 3. Java ---------------------------------------------------------------------
sudo apt install -y openjdk-17-jdk

if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then   # macOS: ask the system where Java is
  JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"
fi
if [ -n "${JAVA_HOME:-}" ]; then                    # put that Java first on the PATH
  export export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
  echo "JAVA_HOME set to $JAVA_HOME"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if ! java -version >/dev/null 2>&1; then            # no Java at all -> clear error message
  echo "ERROR: Java not found. Install Java 17 or newer (macOS: 'brew install openjdk@17')"
  echo "       and/or set JAVA_HOME in the runner's .env file."
  exit 1
fi

# --- 4. Hand over to the next workflow steps -----------------------------------
if [ -n "${GITHUB_ENV:-}" ]; then                   # only true inside GitHub Actions
  echo "$VENV/bin" >> "$GITHUB_PATH"                # later steps use the venv's python/pytest
  echo "VIRTUAL_ENV=$VENV" >> "$GITHUB_ENV"
  if [ -n "${JAVA_HOME:-}" ]; then
    echo "JAVA_HOME=$JAVA_HOME" >> "$GITHUB_ENV"
    echo "$JAVA_HOME/bin" >> "$GITHUB_PATH"
  fi
fi

# --- Versions in the log (useful when something breaks) ------------------------
echo "Python:  $(python --version 2>&1)"
echo "PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"
echo "Java:    $(java -version 2>&1 | head -n 1)"
echo "Note: PySpark 4.x needs Java 17+, PySpark 3.5 works with Java 8/11/17."
