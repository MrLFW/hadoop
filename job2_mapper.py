#!/usr/bin/env python3
import sys

try:
    peak = sys.argv[1].strip()
except Exception:
    sys.exit(1)

for line in sys.stdin:
    try:
        timestamp = line.strip().split("[", 1)[1].split("]", 1)[0].split(":")
        if timestamp[1] != peak:
            continue
        ip = line.split()[0]
        req = line.split('"')[1].split()
        print(f"{req[1]}\t{ip}")
    except Exception:
        continue
