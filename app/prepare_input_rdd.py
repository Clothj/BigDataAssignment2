#!/usr/bin/env python3
import argparse
import os
import re
import subprocess

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build /input/data as one-partition RDD with <id> <title> <text>."
    )
    parser.add_argument("--docs-path", default="/data", help="HDFS path for input documents (.txt).")
    parser.add_argument("--output-path", default="/input/data", help="HDFS output path for indexer input.")
    parser.add_argument("--min-docs", type=int, default=100, help="Minimum number of records required.")
    return parser.parse_args()


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def delete_hdfs_path(path):
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", path], check=False)


def parse_document_record(item):
    file_path, raw_text = item
    base_name = os.path.basename(file_path)
    if not base_name.endswith(".txt"):
        return None

    stem = base_name[:-4]
    if "_" in stem:
        doc_id, title_part = stem.split("_", 1)
    else:
        doc_id, title_part = stem, stem

    doc_id = doc_id.strip()
    title = title_part.replace("_", " ").strip()
    text = normalize_whitespace(raw_text).replace("\t", " ")
    title = normalize_whitespace(title).replace("\t", " ")

    if not doc_id or not title or not text:
        return None

    return f"{doc_id}\t{title}\t{text}"


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("prepare-input-rdd").getOrCreate()
    sc = spark.sparkContext

    try:
        docs_glob = f"{args.docs_path.rstrip('/')}" + "/*.txt"
        docs_rdd = sc.wholeTextFiles(docs_glob)
        prepared_rdd = docs_rdd.map(parse_document_record).filter(lambda x: x is not None)

        count = prepared_rdd.count()
        if count < args.min_docs:
            raise RuntimeError(
                f"Only {count} docs parsed from {args.docs_path}; expected at least {args.min_docs}."
            )

        delete_hdfs_path(args.output_path)
        prepared_rdd.coalesce(1).saveAsTextFile(args.output_path)
        print(
            f"Created {args.output_path} as one partition with {count} records in format "
            f"<doc_id>\\t<doc_title>\\t<doc_text>."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
