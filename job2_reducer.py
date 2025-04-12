#!/usr/bin/env python3
import sys

data = {}
for line in sys.stdin:
    try:
        url, ip = line.strip().split("\t")
        if url in data:
            data[url][0] += 1
            data[url][1].add(ip)
        else:
            data[url] = [1, {ip}]
    except Exception:
        continue
for url, (cnt, ips) in data.items():
    print(f"{url}\t{cnt}\t{len(ips)}")
