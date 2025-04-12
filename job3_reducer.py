#!/usr/bin/env python3
import sys

data = []
for line in sys.stdin:
    try:
        url, count, unique_visitors = line.strip().split("\t")
        data.append((url, int(count), int(unique_visitors)))
    except Exception:
        continue

data.sort(key=lambda x: (-x[1], x[0]))
for url, count, unique_visitors in data[:10]:
    print(
        f"Number of requests: {count}\tURL: {url}\tNumber of unique visitors: {unique_visitors}"
    )
