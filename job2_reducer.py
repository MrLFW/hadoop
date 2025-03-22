#!/usr/bin/env python3
import sys

url_hits = {}
url_ips = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 3:
        continue
    record_type, url, value = parts
    if record_type == "URL":
        url_hits[url] = url_hits.get(url, 0) + int(value)
    elif record_type == "IP":
        if url not in url_ips:
            url_ips[url] = set()
        url_ips[url].add(value)

for url in url_hits:
    hits = url_hits[url]
    unique_visitors = len(url_ips.get(url, set()))
    print(f"{url}\t{hits}\t{unique_visitors}")
