#!/usr/bin/env python3
import sys

count = 0
for line in sys.stdin:
    line = line.strip()
    parts = line.split("\t")
    if len(parts) == 3 and count < 10:
        total_hits, url, unique = parts
        print(
            f"Number of requests: {total_hits}\tURL: {url}\tNumber of unique visitors: {unique}"
        )
        count += 1
    elif count >= 10:
        break
