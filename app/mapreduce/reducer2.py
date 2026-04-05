#!/usr/bin/env python3
import sys
from typing import List


def emit(key: str, values: List[str]) -> None:
    if key.startswith("VOCAB|"):
        term = key.split("|", 1)[1]
        df = 0
        for value in values:
            try:
                df += int(value)
            except ValueError:
                continue
        print(f"VOCAB\t{term}\t{df}")
        return

    if key == "STAT|CORPUS":
        total_docs = 0
        total_doc_len = 0
        for value in values:
            if "|" not in value:
                continue
            doc_count_text, doc_len_text = value.split("|", 1)
            try:
                total_docs += int(doc_count_text)
                total_doc_len += int(doc_len_text)
            except ValueError:
                continue

        avgdl = (float(total_doc_len) / float(total_docs)) if total_docs else 0.0
        print(f"CORPUS\tN\t{total_docs}")
        print(f"CORPUS\tTOTAL_DOC_LEN\t{total_doc_len}")
        print(f"CORPUS\tAVGDL\t{avgdl:.8f}")


def main() -> None:
    current_key = None
    bucket = []

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line or "\t" not in line:
            continue

        key, value = line.split("\t", 1)

        if current_key is None:
            current_key = key
            bucket = [value]
            continue

        if key == current_key:
            bucket.append(value)
            continue

        emit(current_key, bucket)
        current_key = key
        bucket = [value]

    if current_key is not None:
        emit(current_key, bucket)


if __name__ == "__main__":
    main()
