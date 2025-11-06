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

# Choose python interpreter
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Error: python executable not found on PATH." >&2
  exit 1
fi

# Create virtual environment if needed
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating Python virtual environment..."
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

# Determine activation/paths for POSIX vs Windows layouts
if [[ -d "${VENV_DIR}/bin" ]]; then
  ACTIVATE_SCRIPT="${VENV_DIR}/bin/activate"
  VENV_PYTHON="${VENV_DIR}/bin/python"
elif [[ -d "${VENV_DIR}/Scripts" ]]; then
  ACTIVATE_SCRIPT="${VENV_DIR}/Scripts/activate"
  VENV_PYTHON="${VENV_DIR}/Scripts/python.exe"
else
  echo "Error: Could not locate virtualenv activation script in ${VENV_DIR}." >&2
  exit 1
fi

# Activate virtual environment
# shellcheck disable=SC1090
source "${ACTIVATE_SCRIPT}"

# Install dependencies if first run or requirements changed
"${VENV_PYTHON}" -m pip install --upgrade pip >/dev/null
"${VENV_PYTHON}" -m pip install -r "${REQUIREMENTS_FILE}"

cd "${SCRIPT_DIR}"

"${VENV_PYTHON}" "${RUNNER}" "$@"

