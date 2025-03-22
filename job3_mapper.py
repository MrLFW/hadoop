#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 3:
        continue
    url, total, unique = parts
    try:
        total_int = int(total)
    except Exception:
        continue
    # use negative total to sort for top 10
    print(f"{-total_int}\t{url}\t{unique}")
