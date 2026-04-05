#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"

echo "Starting index pipeline for input path: $INPUT_PATH"
bash create_index.sh "$INPUT_PATH"

echo "Storing index data in Cassandra/ScyllaDB"
bash store_index.sh

echo "Index pipeline completed."
