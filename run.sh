#!/bin/bash
set -e
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q -r requirements.txt

echo "Starting UI → http://localhost:8501"
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false streamlit run src/app.py --server.headless true
