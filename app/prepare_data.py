#!/usr/bin/env python3
import argparse
import os
import re
import shutil

from pathvalidate import sanitize_filename
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Prepare plain-text docs from a parquet dataset.")
    parser.add_argument("--parquet", default="/a.parquet", help="Input parquet path.")
    parser.add_argument("--output-dir", default="data", help="Local output folder for .txt documents.")
    parser.add_argument("--n-docs", type=int, default=100, help="Target number of documents.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling.")
    return parser.parse_args()


def normalize_whitespace(text):
    return re.sub(r"\s+", " ", text).strip()


def build_filename(doc_id, title):
    safe_title = normalize_whitespace(title).replace(" ", "_")
    safe_title = safe_title.encode("ascii", "ignore").decode("ascii")
    if not safe_title:
        safe_title = "untitled"
    safe = sanitize_filename(f"{doc_id}_{safe_title}")
    return f"{safe}.txt"


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("data-preparation")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(args.parquet).select("id", "title", "text")
        df = (
            df.filter(F.col("id").isNotNull())
            .filter(F.col("title").isNotNull())
            .filter(F.col("text").isNotNull())
            .withColumn("title", F.trim(F.col("title")))
            .withColumn("text", F.trim(F.col("text")))
            .filter(F.length(F.col("title")) > 0)
            .filter(F.length(F.col("text")) > 0)
        )

        total_docs = df.count()
        if total_docs == 0:
            raise RuntimeError(f"No rows found in parquet dataset: {args.parquet}")
        if total_docs < args.n_docs:
            raise RuntimeError(
                f"Dataset has only {total_docs} non-empty documents; required {args.n_docs}."
            )

        sampled = df.orderBy(F.rand(args.seed)).limit(args.n_docs)

        if os.path.exists(args.output_dir):
            shutil.rmtree(args.output_dir)
        os.makedirs(args.output_dir, exist_ok=True)

        created = 0
        for row in sampled.toLocalIterator():
            filename = build_filename(row["id"], row["title"])
            output_path = os.path.join(args.output_dir, filename)
            text = normalize_whitespace(row["text"])
            if not text:
                continue
            with open(output_path, "w", encoding="utf-8") as output_file:
                output_file.write(text)
            created += 1

        if created < args.n_docs:
            raise RuntimeError(
                f"Only {created} valid documents were created; required {args.n_docs}."
            )

        print(f"Prepared {created} documents in '{args.output_dir}'.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
