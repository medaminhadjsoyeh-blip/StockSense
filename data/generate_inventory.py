"""Simule data/inventory.csv à partir des ventes historiques.

Les transactions ne donnent ni le stock initial ni le seuil de
réapprovisionnement (cahier des charges §3) : on les simule de façon
reproductible.

Règles (graine fixe) :
- vente moyenne v = quantité vendue sur les 30 derniers jours / 30 ;
- initial_stock = arrondi(v × U(1, 25)) jours de couverture, minimum 5 ;
- reorder_threshold = arrondi(v × 3) (3 jours de ventes), minimum 1 ;
- produit sans vente récente : initial_stock 50, reorder_threshold 5.
Environ un produit actif sur huit démarre donc sous le seuil d'alerte.

Usage : python data/generate_inventory.py --transactions data/processed/retail_clean.csv \\
                                          --out data/inventory.csv
"""

import argparse
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import metrics  # noqa: E402


def build_inventory(daily, seed: int = 42):
    averages = metrics.average_daily_sales(daily)
    rng = random.Random(seed)
    rows = []
    for product_id in sorted(daily["product_id"].unique()):
        avg = float(averages.get(product_id, 0.0))
        if avg > 0:
            initial = max(5, math.ceil(avg * rng.uniform(1, 25)))
            threshold = max(1, math.ceil(avg * metrics.COVERAGE_ALERT_DAYS))
        else:
            initial, threshold = 50, 5
        rows.append({"product_id": product_id, "initial_stock": initial, "reorder_threshold": threshold})
    return rows


def main(argv=None) -> None:
    import pandas as pd

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transactions", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=Path("data/inventory.csv"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    parts = [metrics.aggregate_daily_sales(c) for c in metrics.read_transactions(args.transactions, chunksize=200_000)]
    rows = build_inventory(metrics.combine_daily(parts), args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(args.out, index=False)
    print(f"{len(rows)} produits écrits dans {args.out}")


if __name__ == "__main__":
    main()
