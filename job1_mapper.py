#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line = line.strip()
        parts = line.split("[")
        timestamp = parts[1]
        time_parts = timestamp.split(":")
        hour = time_parts[1]
        print(f"{hour}\t1")
    except Exception:
        continue
