#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split("\t")
        url, total, unique = parts
        print(f"{total}\t{url}\t{unique}")
    except Exception:
        continue
