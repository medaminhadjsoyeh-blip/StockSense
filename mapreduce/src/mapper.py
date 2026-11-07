#!/usr/bin/env python3
import sys
import csv
import os

JOB = os.environ.get("JOB", "product_count")

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        if len(row) < 9 or row[0] == "transaction_id":
            continue

        customer_id = row[2].strip()
        product_id = row[3].strip()
        quantity = float(row[5])
        unit_price = float(row[6])
        total_amount = float(row[7])

        if not product_id:
            continue

        if JOB == "product_count":
            print("{}\t1\t{}".format(product_id, quantity))
        elif JOB == "product_revenue":
            print("{}\t{}".format(product_id, total_amount))
        elif JOB == "customer_total":
            if customer_id:
                print("{}\t{}".format(customer_id, total_amount))
        elif JOB == "average_price":
            print("{}\t{}\t1".format(product_id, unit_price))
        elif JOB == "top10":
            print("{}\t{}".format(product_id, total_amount))

    except (ValueError, IndexError):
        continue
