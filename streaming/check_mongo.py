#!/usr/bin/env python3
"""Resume du contenu MongoDB produit par le streaming."""
from pymongo import MongoClient

db = MongoClient("mongodb://localhost:27017/")["stocksense"]
for name in ["sales_windows", "product_windows", "stock_status", "alerts"]:
    print(name, ":", db[name].count_documents({}))
print("Ventes comptees :", sum(d["sales_count"] for d in db.sales_windows.find()))
print("Alertes par regle :")
for row in db.alerts.aggregate([{"$group": {"_id": "$rule", "n": {"$sum": 1}}}]):
    print("  ", row["_id"], ":", row["n"])
