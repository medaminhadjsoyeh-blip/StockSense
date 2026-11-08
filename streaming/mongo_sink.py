#!/usr/bin/env python3
"""Ecriture idempotente (upsert avec _id stable) des resultats dans MongoDB."""
import csv
import hashlib
from pathlib import Path

from pymongo import MongoClient, UpdateOne

ROOT = Path(__file__).resolve().parents[1]
COVERAGE_ALERT_DAYS = 3  # alerte si moins de 3 jours de couverture
MOTIFS = {
    "message_invalide": "Message illisible ou champ obligatoire manquant",
    "quantite_elevee": "Quantite superieure au seuil",
    "prix_negatif": "Prix negatif ou nul",
    "prix_eloigne": "Prix eloigne de plus de 50 % du prix de reference",
    "montant_incoherent": "Montant different de quantite x prix",
    "id_duplique": "Identifiant de transaction repete",
    "stock_faible": "Couverture de stock inferieure a 3 jours",
}

db = MongoClient("mongodb://localhost:27017/")["stocksense"]


def upsert(collection, documents):
    operations = [
        UpdateOne({"_id": doc["_id"]},
                  {"$set": {k: v for k, v in doc.items() if k != "_id"}},
                  upsert=True)
        for doc in documents
    ]
    if operations:
        db[collection].bulk_write(operations, ordered=False)


def load_inventory():
    with (ROOT / "data/inventory.csv").open(encoding="utf-8") as file:
        return {row["product_id"]: {
            "initial_stock": int(row["initial_stock"]),
            "reorder_threshold": int(row["reorder_threshold"]),
            "avg_daily_sales": float(row["avg_daily_sales"]),
        } for row in csv.DictReader(file)}


INVENTORY = load_inventory()


def stock_document(product_id, sold):
    item = INVENTORY[product_id]
    available = max(item["initial_stock"] - sold, 0)
    days = round(available / item["avg_daily_sales"], 2)
    return {
        "_id": product_id,
        "product_id": product_id,
        "initial_stock": item["initial_stock"],
        "quantity_sold": sold,
        "available_stock": available,
        "avg_daily_sales": item["avg_daily_sales"],
        "coverage_days": days,
        "reorder_threshold": item["reorder_threshold"],
        "reorder_needed": available <= item["reorder_threshold"],
        "low_coverage": days < COVERAGE_ALERT_DAYS,
    }


def stock_alert(doc):
    return {
        "_id": "stock_faible|" + doc["product_id"],
        "rule": "stock_faible",
        "motif": MOTIFS["stock_faible"],
        "product_id": doc["product_id"],
        "available_stock": doc["available_stock"],
        "coverage_days": doc["coverage_days"],
    }


def init_stock():
    """Cree les produits absents sans ecraser les ventes deja comptees."""
    operations = []
    alerts = []
    for product_id in INVENTORY:
        doc = stock_document(product_id, 0)
        fields = {k: v for k, v in doc.items() if k != "_id"}
        operations.append(UpdateOne({"_id": product_id},
                                    {"$setOnInsert": fields}, upsert=True))
        if doc["low_coverage"]:
            alerts.append(stock_alert(doc))
    db.stock_status.bulk_write(operations, ordered=False)
    upsert("alerts", alerts)


def write_alerts(rows):
    documents = []
    for row in rows:
        key = row["transaction_id"] or hashlib.sha256(row["raw"].encode("utf-8")).hexdigest()
        doc = {name: row[name] for name in row if name != "raw"}
        doc["_id"] = row["rule"] + "|" + key
        doc["motif"] = MOTIFS[row["rule"]]
        documents.append(doc)
    upsert("alerts", documents)


def write_duplicates(rows):
    upsert("alerts", [{
        "_id": "id_duplique|" + row["transaction_id"],
        "rule": "id_duplique",
        "motif": MOTIFS["id_duplique"],
        "transaction_id": row["transaction_id"],
        "occurrences": row["occurrences"],
    } for row in rows])


def write_sales_windows(rows):
    upsert("sales_windows", [{
        "_id": row["window_start"].isoformat(),
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "sales_count": row["sales_count"],
        "revenue": row["revenue"],
        "quantity": row["quantity"],
    } for row in rows])


def save_product_windows(rows):
    upsert("product_windows", [{
        "_id": row["product_id"] + "|" + row["window_start"].isoformat(),
        "product_id": row["product_id"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "sales_count": row["sales_count"],
        "revenue": row["revenue"],
        "quantity": row["quantity"],
    } for row in rows])


def refresh_stock(product_ids):
    """Recalcule le stock a partir des fenetres deja dedupliquees."""
    totals = db.product_windows.aggregate([
        {"$match": {"product_id": {"$in": list(product_ids)}}},
        {"$group": {"_id": "$product_id", "sold": {"$sum": "$quantity"}}},
    ])
    documents = [stock_document(row["_id"], row["sold"])
                 for row in totals if row["_id"] in INVENTORY]
    upsert("stock_status", documents)
    upsert("alerts", [stock_alert(doc) for doc in documents if doc["low_coverage"]])


def write_product_windows(rows):
    save_product_windows(rows)
    if rows:
        refresh_stock({row["product_id"] for row in rows})
