#!/usr/bin/env python3
import sys


def emit(key: str, total: int) -> None:
    parts = key.split("|", 2)
    if len(parts) < 3:
        return

    record_type = parts[0]
    if record_type == "TERM":
        term = parts[1]
        doc_id = parts[2]
        print(f"TERM\t{term}\t{doc_id}\t{total}")
    elif record_type == "DOC":
        doc_id = parts[1]
        title = parts[2]
        print(f"DOC\t{doc_id}\t{title}\t{total}")


def main() -> None:
    current_key = None
    running_total = 0

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line or "\t" not in line:
            continue

        key, value_text = line.split("\t", 1)
        try:
            value = int(value_text)
        except ValueError:
            continue

        if current_key is None:
            current_key = key
            running_total = value
            continue

        if key == current_key:
            running_total += value
            continue

        emit(current_key, running_total)
        current_key = key
        running_total = value

    if current_key is not None:
        emit(current_key, running_total)


if __name__ == "__main__":
    main()
