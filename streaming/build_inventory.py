#!/usr/bin/env python3
"""Cree data/inventory.csv : inventaire SYNTHETIQUE (les ventes ne donnent pas le stock)."""
import csv
import json
import math
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAYS_IN_HISTORY = 374      # du 01/12/2010 au 09/12/2011
LOW_STOCK_SHARE = 0.05     # part des produits presque en rupture
REORDER_DAYS = 7           # seuil de reapprovisionnement, en jours de vente

rng = random.Random(42)    # meme fichier a chaque execution
products = json.loads((ROOT / "results/product_count.json").read_text(encoding="utf-8"))

rows = []
for product in products:
    if not product["product_id"][:5].isdigit():
        continue
    daily = product["total_quantity"] / DAYS_IN_HISTORY
    if rng.random() < LOW_STOCK_SHARE:
        coverage = rng.uniform(1, 4)
    else:
        coverage = rng.uniform(10, 60)
    rows.append({
        "product_id": product["product_id"],
        "initial_stock": max(1, math.ceil(daily * coverage)),
        "reorder_threshold": max(1, math.ceil(daily * REORDER_DAYS)),
        "avg_daily_sales": round(daily, 4),
    })

output = ROOT / "data/inventory.csv"
output.parent.mkdir(parents=True, exist_ok=True)
with output.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

print(len(rows), "produits ecrits dans", output)
