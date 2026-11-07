#!/usr/bin/env python3
import sys
import os

JOB = os.environ.get("JOB", "product_count")

def groups():
    current_key = None
    values = []

    for line in sys.stdin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 2:
            continue

        key = parts[0]

        if current_key is not None and key != current_key:
            yield current_key, values
            values = []

        current_key = key
        values.append(parts[1:])

    if current_key is not None:
        yield current_key, values


if JOB == "product_count":
    for key, values in groups():
        count = sum(int(v[0]) for v in values)
        quantity = sum(float(v[1]) for v in values)
        print("{}\t{}\t{}".format(key, count, quantity))

elif JOB in ("product_revenue", "customer_total"):
    for key, values in groups():
        total = sum(float(v[0]) for v in values)
        print("{}\t{}".format(key, total))

elif JOB == "average_price":
    for key, values in groups():
        total = sum(float(v[0]) for v in values)
        count = sum(int(v[1]) for v in values)
        if count:
            print("{}\t{}".format(key, total / count))

elif JOB == "top10":
    ranking = []

    for key, values in groups():
        total = sum(float(v[0]) for v in values)
        ranking.append((key, total))

    ranking.sort(key=lambda x: x[1], reverse=True)

    for rank, item in enumerate(ranking[:10], 1):
        print("{}\t{}\t{}".format(rank, item[0], item[1]))
