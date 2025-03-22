#!/usr/bin/env python3
import sys

if len(sys.argv) < 2:
    sys.stderr.write("Usage: job2_mapper.py <peak_hour>\n")
    sys.exit(1)
peak_hour = sys.argv[1]

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue
        timestamp = line.split("[")[1]
        hour = timestamp.split(":")[1]
        if hour != peak_hour:  # only prints lines from the peak hour
            continue
        ip = line.split()[0]
        request = line.split('"')[1]
        url = request.split()[1]

        # print for reducer to count hits on each URL
        print(f"URL\t{url}\t1")
        # print unique visitors for reducer to count
        print(f"IP\t{url}\t{ip}")

    except (IndexError, ValueError):
        # Skip any malformed lines
        continue
