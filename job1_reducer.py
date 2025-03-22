#!/usr/bin/env python3
import sys

counts = {}
for line in sys.stdin:
    try:
        line = line.strip()
        hour, count = line.split("\t")
        count = int(count)
        counts[hour] = counts.get(hour, 0) + count
    except Exception:
        continue

for hour, total in counts.items():
    print(f"{hour}\t{total}")
