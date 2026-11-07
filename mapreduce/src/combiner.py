#!/usr/bin/env python3
import sys
import os

JOB = os.environ.get("JOB", "product_count")

current_key = None
total1 = 0.0
total2 = 0.0

def emit():
    if current_key is None:
        return

    if JOB == "product_count":
        print("{}\t{}\t{}".format(
            current_key, int(total1), total2))
    elif JOB == "average_price":
        print("{}\t{}\t{}".format(
            current_key, total1, int(total2)))
    else:
        print("{}\t{}".format(current_key, total1))


for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")

    if len(parts) < 2:
        continue

    key = parts[0]

    if current_key is not None and key != current_key:
        emit()
        total1 = 0.0
        total2 = 0.0

    current_key = key

    try:
        total1 += float(parts[1])
        if JOB in ("product_count", "average_price"):
            total2 += float(parts[2])
    except (ValueError, IndexError):
        continue

emit()
