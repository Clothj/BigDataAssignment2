#!/bin/bash
set -euo pipefail

# Start ssh server and cluster services
service ssh restart
bash start-services.sh

# Create and package Python runtime for YARN executors
rm -rf .venv
rm -f .venv.tar.gz
python3 -m venv .venv
source .venv/bin/activate
pip install --no-cache-dir --progress-bar off -r requirements.txt
venv-pack -o .venv.tar.gz

# Prepare data in HDFS
export N_DOCS="${N_DOCS:-100}"
bash prepare_data.sh

# Build index and store it in Cassandra
bash index.sh /input/data

# Run a sample query by default (can be overridden by environment variables)
if [[ "${RUN_SAMPLE_QUERY:-0}" == "1" ]]; then
  bash search.sh "${SAMPLE_QUERY:-this is a query}"
fi
