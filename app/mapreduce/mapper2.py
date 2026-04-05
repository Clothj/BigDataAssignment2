#!/usr/bin/env python3
import sys


def main() -> None:
    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t")
        if not parts:
            continue

        record_type = parts[0]

        if record_type == "TERM" and len(parts) >= 4:
            term = parts[1]
            print(f"VOCAB|{term}\t1")
        elif record_type == "DOC" and len(parts) >= 4:
            doc_len = parts[3]
            print(f"STAT|CORPUS\t1|{doc_len}")


if __name__ == "__main__":
    main()
