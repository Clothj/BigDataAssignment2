#!/bin/bash
set -euo pipefail

INDEXER_BASE="${1:-/indexer}"

source .venv/bin/activate

echo "Loading index data from HDFS path: $INDEXER_BASE"
python app.py "$INDEXER_BASE"
