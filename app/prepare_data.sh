#!/bin/bash
set -euo pipefail

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

N_DOCS="${N_DOCS:-100}"
DOCS_HDFS_PATH="/data"
INPUT_HDFS_PATH="/input/data"
DATA_SRC_DIR="data"

if [[ -f "a.parquet" ]]; then
  echo "Uploading parquet to HDFS: /a.parquet"
  hdfs dfs -put -f a.parquet /a.parquet

  echo "Generating ${N_DOCS} plain-text documents with PySpark"
  spark-submit prepare_data.py --parquet /a.parquet --output-dir data --n-docs "${N_DOCS}"
else
  echo "a.parquet was not found in /app. Falling back to existing ./data documents."
  rm -rf data_fallback
  mkdir -p data_fallback
  python3 - "${N_DOCS}" <<'PY'
import os
import shutil
import sys

n_docs = int(sys.argv[1])
src = "data"
dst = "data_fallback"

count = 0
for name in sorted(os.listdir(src)):
    if not name.endswith(".txt"):
        continue
    if not name.isascii():
        continue
    source_path = os.path.join(src, name)
    if os.path.getsize(source_path) <= 0:
        continue
    shutil.copy2(source_path, os.path.join(dst, name))
    count += 1
    if count >= n_docs:
        break

if count < n_docs:
    raise SystemExit(f"Not enough fallback docs: {count}/{n_docs}")

print(f"Prepared fallback documents: {count}")
PY
  DATA_SRC_DIR="data_fallback"
fi

echo "Storing prepared documents in HDFS: ${DOCS_HDFS_PATH}"
hdfs dfs -rm -r -f "${DOCS_HDFS_PATH}" || true
hdfs dfs -mkdir -p "${DOCS_HDFS_PATH}"
hdfs dfs -put -f "${DATA_SRC_DIR}"/*.txt "${DOCS_HDFS_PATH}/"

echo "Building one-partition indexer input in HDFS: ${INPUT_HDFS_PATH}"
spark-submit prepare_input_rdd.py --docs-path "${DOCS_HDFS_PATH}" --output-path "${INPUT_HDFS_PATH}" --min-docs "${N_DOCS}"

echo "Sample of /data:"
hdfs dfs -ls "${DOCS_HDFS_PATH}"

echo "Sample of /input/data:"
hdfs dfs -cat "${INPUT_HDFS_PATH}/part-*" | awk 'NR<=5'

echo "Data preparation completed."
