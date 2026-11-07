import json
import os
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["stocksense"]

files = {
    "product_count": "product_count.json",
    "product_revenue": "product_revenue.json",
    "customer_total": "customer_total.json",
    "average_price": "average_price.json",
    "top10": "top10.json"
}

for collection_name, filename in files.items():
    path = os.path.join("results", filename)

    with open(path, "r", encoding="utf-8") as f:
        documents = json.load(f)

    collection = db[collection_name]
    collection.delete_many({})

    if documents:
        collection.insert_many(documents)

    print("{} : {} documents importés".format(
        collection_name, collection.count_documents({})
    ))

# Importer le rapport qualité
with open("results/data_quality_report.json", "r", encoding="utf-8") as f:
    report = json.load(f)

db.data_quality_report.delete_many({})
db.data_quality_report.insert_one(report)

print("data_quality_report : {} document importé".format(
    db.data_quality_report.count_documents({})
))

client.close()
print("\nImportation MongoDB terminée.")
