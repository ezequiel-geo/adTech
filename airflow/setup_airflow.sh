#!/bin/bash
set -e

# ─────────────────────────────────────────────
# Airflow 3.1.8 local setup script
# Requires Python 3.12
# Run from the repo root: bash airflow/setup_airflow.sh
# ─────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/airflow_adtech_env"

export AIRFLOW_HOME="$SCRIPT_DIR"

echo "→ AIRFLOW_HOME: $AIRFLOW_HOME"
echo "→ Virtualenv:   $VENV_DIR"
echo ""

# 1. Create virtualenv with Python 3.12
if [ ! -d "$VENV_DIR" ]; then
  echo "→ Creating virtualenv (Python 3.12)..."
  python3.12 -m venv "$VENV_DIR"
else
  echo "→ Virtualenv already exists, skipping creation."
fi

source "$VENV_DIR/bin/activate"

# 2. Install Airflow 3.1.8 with official constraints for Python 3.12
echo ""
echo "→ Installing Airflow 3.1.8 (this may take a few minutes)..."
pip install --quiet --upgrade pip
pip install "apache-airflow==3.1.8" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt"

# 3. Install pipeline dependencies
echo ""
echo "→ Installing pipeline dependencies..."
pip install --quiet -r "$SCRIPT_DIR/requirements.txt"

# 4. Init the metadata database
echo ""
echo "→ Initialising Airflow database..."
airflow db migrate

echo ""
echo "✓ Setup complete."
echo ""
echo "To start Airflow, run:"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  source $VENV_DIR/bin/activate"
echo "  airflow standalone"
echo ""
echo "The admin password will be printed on first startup."
echo "Or check: cat \$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
