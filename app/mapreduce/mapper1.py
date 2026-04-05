#!/usr/bin/env python3
import re
import sys
from collections import Counter


TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")


def tokenize(text: str):
    return TOKEN_RE.findall(text.lower())


def parse_input_line(line: str):
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) != 3:
        return None

    doc_id = parts[0].strip()
    title = parts[1].strip().replace("|", " ").replace("\t", " ")
    text = parts[2].strip()

    if not doc_id or not title or not text:
        return None

    return doc_id, title, text


def main() -> None:
    for line in sys.stdin:
        parsed = parse_input_line(line)
        if parsed is None:
            continue

        doc_id, title, text = parsed
        counter = Counter(tokenize(text))
        doc_len = sum(counter.values())
        if doc_len == 0:
            continue

        print(f"DOC|{doc_id}|{title}\t{doc_len}")
        for term, tf in counter.items():
            print(f"TERM|{term}|{doc_id}\t{tf}")


if __name__ == "__main__":
    main()
