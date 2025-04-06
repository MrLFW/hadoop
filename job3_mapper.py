#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split("\t")
        url, total, unique = parts
        total = int(total)
        print(f"{total}\t{url}\t{unique}")
    except Exception:
        continue
