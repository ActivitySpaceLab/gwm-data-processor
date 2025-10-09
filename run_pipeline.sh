#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLKIT_DIR="${SCRIPT_DIR}/pipeline_toolkit"
VENV_DIR="${TOOLKIT_DIR}/venv"
ENV_FILE="${TOOLKIT_DIR}/.env"
REQUIREMENTS_FILE="${TOOLKIT_DIR}/requirements.txt"
RUNNER="${TOOLKIT_DIR}/pipeline_runner.py"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Error: ${ENV_FILE} not found." >&2
  echo "Copy pipeline_toolkit/.env.example to pipeline_toolkit/.env and fill in your secrets." >&2
  exit 1
fi

# Load environment variables from .env
set -o allexport
source "${ENV_FILE}"
set +o allexport

# Create virtual environment if needed
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating Python virtual environment..."
  python3 -m venv "${VENV_DIR}"
fi

# Activate virtual environment
source "${VENV_DIR}/bin/activate"

# Install dependencies if first run or requirements changed
pip install --upgrade pip >/dev/null
pip install -r "${REQUIREMENTS_FILE}"

cd "${SCRIPT_DIR}"

python3 "${RUNNER}" "$@"

