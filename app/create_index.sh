#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"
TMP_BASE="/tmp/indexer"
INDEXER_BASE="/indexer"

find_streaming_jar() {
  local jar_path
  jar_path="$(find "$HADOOP_HOME/share/hadoop" -name 'hadoop-streaming*.jar' | head -n 1)"
  if [[ -z "$jar_path" ]]; then
    echo "Could not find hadoop-streaming jar under \$HADOOP_HOME/share/hadoop" >&2
    exit 1
  fi
  echo "$jar_path"
}

STREAMING_JAR="$(find_streaming_jar)"

echo "Using Hadoop streaming jar: $STREAMING_JAR"
echo "Creating index from input path: $INPUT_PATH"

hdfs dfs -test -e "$INPUT_PATH" || {
  echo "Input path does not exist in HDFS: $INPUT_PATH" >&2
  exit 1
}

hdfs dfs -rm -r -f "$TMP_BASE" || true
hdfs dfs -rm -r -f "$INDEXER_BASE" || true

hdfs dfs -mkdir -p "$TMP_BASE"
hdfs dfs -mkdir -p "$INDEXER_BASE/index" "$INDEXER_BASE/documents" "$INDEXER_BASE/vocabulary" "$INDEXER_BASE/stats"

echo "Running pipeline 1: postings + document stats"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline-1" \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$TMP_BASE/pipeline1"

echo "Running pipeline 2: vocabulary DF + corpus stats"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline-2" \
  -files mapreduce/mapper2.py,mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input "$TMP_BASE/pipeline1" \
  -output "$TMP_BASE/pipeline2"

echo "Materializing final /indexer datasets"
LOCAL_TMP="$(mktemp -d)"
trap 'rm -rf "$LOCAL_TMP"' EXIT

hdfs dfs -cat "$TMP_BASE/pipeline1/part-*" | awk -F '\t' '$1=="TERM"{print $0}' > "$LOCAL_TMP/index.tsv"
hdfs dfs -cat "$TMP_BASE/pipeline1/part-*" | awk -F '\t' '$1=="DOC"{print $0}' > "$LOCAL_TMP/documents.tsv"
hdfs dfs -cat "$TMP_BASE/pipeline2/part-*" | awk -F '\t' '$1=="VOCAB"{print $0}' > "$LOCAL_TMP/vocabulary.tsv"
hdfs dfs -cat "$TMP_BASE/pipeline2/part-*" | awk -F '\t' '$1=="CORPUS"{print $0}' > "$LOCAL_TMP/stats.tsv"

hdfs dfs -put -f "$LOCAL_TMP/index.tsv" "$INDEXER_BASE/index/part-00000"
hdfs dfs -put -f "$LOCAL_TMP/documents.tsv" "$INDEXER_BASE/documents/part-00000"
hdfs dfs -put -f "$LOCAL_TMP/vocabulary.tsv" "$INDEXER_BASE/vocabulary/part-00000"
hdfs dfs -put -f "$LOCAL_TMP/stats.tsv" "$INDEXER_BASE/stats/part-00000"

echo "Index creation completed."
hdfs dfs -ls -R "$INDEXER_BASE"
