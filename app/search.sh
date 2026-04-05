#!/bin/bash
set -euo pipefail

QUERY="${*:-}"
if [[ -z "$QUERY" ]]; then
  echo "Usage: bash search.sh \"your query text\""
  exit 1
fi

source .venv/bin/activate

# Python for the driver and executors packed in .venv.tar.gz
export PYSPARK_DRIVER_PYTHON="$(which python)"
export PYSPARK_PYTHON="./.venv/bin/python"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.yarn.jars=local:///usr/local/spark/jars/* \
  --archives /app/.venv.tar.gz#.venv \
  query.py "$QUERY"
