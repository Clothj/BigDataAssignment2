#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from typing import Iterable, Iterator

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


KEYSPACE = os.environ.get("INDEX_KEYSPACE", "search_engine")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra-server")


def iter_hdfs_lines(path_glob: str) -> Iterator[str]:
    process = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path_glob],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert process.stdout is not None
    for line in process.stdout:
        line = line.rstrip("\n")
        if line:
            yield line

    stderr = ""
    if process.stderr is not None:
        stderr = process.stderr.read().strip()

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"Failed reading HDFS path {path_glob}: {stderr}")


def wait_for_cassandra(host: str, retries: int = 24, sleep_seconds: int = 5):
    last_error = None
    for _ in range(retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            return cluster, session
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Could not connect to Cassandra host {host}: {last_error}")


def execute_chunked(session, prepared, args: Iterable[tuple], chunk_size: int = 512):
    chunk = []
    for values in args:
        chunk.append(values)
        if len(chunk) >= chunk_size:
            for success, result in execute_concurrent_with_args(session, prepared, chunk, concurrency=64):
                if not success:
                    raise RuntimeError(result)
            chunk = []

    if chunk:
        for success, result in execute_concurrent_with_args(session, prepared, chunk, concurrency=64):
            if not success:
                raise RuntimeError(result)


def parse_index_rows(lines: Iterable[str]):
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 4 or parts[0] != "TERM":
            continue
        term = parts[1]
        doc_id = parts[2]
        try:
            tf = int(parts[3])
        except ValueError:
            continue
        yield (term, doc_id, tf)


def parse_document_rows(lines: Iterable[str]):
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 4 or parts[0] != "DOC":
            continue
        doc_id = parts[1]
        title = parts[2]
        try:
            doc_len = int(parts[3])
        except ValueError:
            continue
        yield (doc_id, title, doc_len)


def parse_vocabulary_rows(lines: Iterable[str]):
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 3 or parts[0] != "VOCAB":
            continue
        term = parts[1]
        try:
            df = int(parts[2])
        except ValueError:
            continue
        yield (term, df)


def parse_stats_rows(lines: Iterable[str]):
    for line in lines:
        parts = line.split("\t")
        if len(parts) < 3 or parts[0] != "CORPUS":
            continue
        stat = parts[1]
        try:
            value = float(parts[2])
        except ValueError:
            continue
        yield (stat, value)


def main() -> None:
    base_path = sys.argv[1] if len(sys.argv) > 1 else "/indexer"

    cluster, session = wait_for_cassandra(CASSANDRA_HOST)

    try:
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        session.set_keyspace(KEYSPACE)

        session.execute("CREATE TABLE IF NOT EXISTS vocabulary (term text PRIMARY KEY, df int)")
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                doc_id text PRIMARY KEY,
                title text,
                doc_len int
            )
            """
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS postings (
                term text,
                doc_id text,
                tf int,
                PRIMARY KEY ((term), doc_id)
            )
            """
        )
        session.execute(
            """
            CREATE TABLE IF NOT EXISTS corpus_stats (
                stat text PRIMARY KEY,
                value double
            )
            """
        )

        session.execute("TRUNCATE vocabulary")
        session.execute("TRUNCATE documents")
        session.execute("TRUNCATE postings")
        session.execute("TRUNCATE corpus_stats")

        insert_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
        insert_doc = session.prepare("INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)")
        insert_posting = session.prepare("INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)")
        insert_stat = session.prepare("INSERT INTO corpus_stats (stat, value) VALUES (?, ?)")

        execute_chunked(
            session,
            insert_posting,
            parse_index_rows(iter_hdfs_lines(f"{base_path}/index/part-*")),
        )
        execute_chunked(
            session,
            insert_doc,
            parse_document_rows(iter_hdfs_lines(f"{base_path}/documents/part-*")),
        )
        execute_chunked(
            session,
            insert_vocab,
            parse_vocabulary_rows(iter_hdfs_lines(f"{base_path}/vocabulary/part-*")),
        )
        execute_chunked(
            session,
            insert_stat,
            parse_stats_rows(iter_hdfs_lines(f"{base_path}/stats/part-*")),
        )

        print(f"Index data loaded successfully into keyspace '{KEYSPACE}'.")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
