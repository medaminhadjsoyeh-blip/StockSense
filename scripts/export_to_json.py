import csv
import json
import os

BASE = "results"

def read_tsv(filename, converter):
    path = os.path.join(BASE, filename)
    data = []

    with open(path, "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter="\t")
        for row in reader:
            if row:
                data.append(converter(row))

    return data

exports = {
    "product_count": read_tsv(
        "product_count.tsv",
        lambda r: {
            "product_id": r[0],
            "transaction_count": int(r[1]),
            "total_quantity": round(float(r[2]), 2)
        }
    ),

    "product_revenue": read_tsv(
        "product_revenue.tsv",
        lambda r: {
            "product_id": r[0],
            "total_revenue": round(float(r[1]), 2)
        }
    ),

    "customer_total": read_tsv(
        "customer_total.tsv",
        lambda r: {
            "customer_id": r[0],
            "total_spent": round(float(r[1]), 2)
        }
    ),

    "average_price": read_tsv(
        "average_price.tsv",
        lambda r: {
            "product_id": r[0],
            "average_price": round(float(r[1]), 4)
        }
    ),

    "top10": read_tsv(
        "top10.tsv",
        lambda r: {
            "rank": int(r[0]),
            "product_id": r[1],
            "total_revenue": round(float(r[2]), 2)
        }
    )
}

for name, documents in exports.items():
    path = os.path.join(BASE, name + ".json")

    with open(path, "w", encoding="utf-8") as file:
        json.dump(documents, file, ensure_ascii=False, indent=2)

    print("{} : {} documents exportés".format(name, len(documents)))

print("\nExport JSON terminé.")
