#!/usr/bin/env python3
import math
import os
import re
import sys
import time

from cassandra.cluster import Cluster
from cassandra.protocol import InvalidRequest
from pyspark.sql import SparkSession


CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra-server")
KEYSPACE = os.environ.get("INDEX_KEYSPACE", "search_engine")
K1 = 1.2
B = 0.75
TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


def read_query() -> str:
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    return sys.stdin.read().strip()


def wait_for_cassandra(host: str, retries: int = 24, sleep_seconds: int = 5):
    last_error = None
    for _ in range(retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            try:
                session.set_keyspace(KEYSPACE)
            except InvalidRequest as exc:
                cluster.shutdown()
                raise RuntimeError(
                    f"Keyspace '{KEYSPACE}' is missing. Run index.sh first."
                ) from exc
            return cluster, session
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Could not connect to Cassandra host {host}: {last_error}")


def bm25_score(tf: int, df: int, doc_len: int, n_docs: float, avgdl: float) -> float:
    if tf <= 0 or df <= 0 or n_docs <= 0:
        return 0.0

    norm = K1 * (1.0 - B + B * (float(doc_len) / max(avgdl, 1e-9)))
    tf_part = (tf * (K1 + 1.0)) / (tf + norm)
    idf = math.log(1.0 + ((n_docs - df + 0.5) / (df + 0.5)))
    return idf * tf_part


def main() -> None:
    query_text = read_query()
    if not query_text:
        print("No query was provided.")
        return

    terms = tokenize(query_text)
    if not terms:
        print("Query does not contain searchable terms.")
        return

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()
    sc = spark.sparkContext

    cluster = None
    session = None

    try:
        cluster, session = wait_for_cassandra(CASSANDRA_HOST)
        df_stmt = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
        postings_stmt = session.prepare("SELECT doc_id, tf FROM postings WHERE term = ?")
        doc_stmt = session.prepare("SELECT title, doc_len FROM documents WHERE doc_id = ?")
        stat_stmt = session.prepare("SELECT value FROM corpus_stats WHERE stat = ?")

        n_docs = 0.0
        avgdl = 0.0

        n_row = session.execute(stat_stmt, ("N",)).one()
        if n_row is not None:
            n_docs = float(n_row.value)

        avgdl_row = session.execute(stat_stmt, ("AVGDL",)).one()
        if avgdl_row is not None:
            avgdl = float(avgdl_row.value)

        if n_docs <= 0:
            print("Index statistics are missing. Run index.sh first.")
            return
        if avgdl <= 0:
            avgdl = 1.0

        unique_terms = sorted(set(terms))
        term_df = {}
        for term in unique_terms:
            row = session.execute(df_stmt, (term,)).one()
            if row is not None:
                term_df[term] = int(row.df)

        if not term_df:
            print("No matching terms found in vocabulary.")
            return

        postings_rows = []
        doc_ids = set()
        for term, df in term_df.items():
            for posting in session.execute(postings_stmt, (term,)):
                doc_id = posting.doc_id
                tf = int(posting.tf)
                postings_rows.append((doc_id, term, tf, df))
                doc_ids.add(doc_id)

        if not postings_rows:
            print("No documents matched the query terms.")
            return

        doc_meta = {}
        for doc_id in doc_ids:
            row = session.execute(doc_stmt, (doc_id,)).one()
            if row is None:
                continue
            doc_meta[doc_id] = (row.title, int(row.doc_len))

        scored_input = []
        for doc_id, term, tf, df in postings_rows:
            if doc_id not in doc_meta:
                continue
            title, doc_len = doc_meta[doc_id]
            scored_input.append((doc_id, title, tf, df, doc_len))

        scores_rdd = (
            sc.parallelize(scored_input)
            .map(
                lambda x: (
                    (x[0], x[1]),
                    bm25_score(
                        tf=x[2],
                        df=x[3],
                        doc_len=x[4],
                        n_docs=n_docs,
                        avgdl=avgdl,
                    ),
                )
            )
            .reduceByKey(lambda a, b: a + b)
        )

        top10 = scores_rdd.takeOrdered(10, key=lambda x: -x[1])

        if not top10:
            print("No documents matched the query terms.")
            return

        for idx, ((doc_id, title), score) in enumerate(top10, start=1):
            print(f"{idx}\t{doc_id}\t{title}\t{score:.6f}")
    finally:
        if cluster is not None:
            cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
