"""Génère un échantillon synthétique de transactions au format nettoyé.

Même schéma que data/processed/retail_clean.csv (scripts/clean_data.py),
déterministe (graine fixe) : sert aux tests et à la démonstration sans le
dataset UCI.

Usage : python data/generate_sample.py [--rows 3000] [--seed 7] [--out data/sample/transactions_sample.csv]
"""

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

COLUMNS = [
    "transaction_id", "timestamp", "customer_id", "product_id", "description",
    "quantity", "unit_price", "total_amount", "country",
]
PRODUCTS = [
    ("P001", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("P002", "REGENCY CAKESTAND 3 TIER", 12.75),
    ("P003", "JUMBO BAG RED RETROSPOT", 1.95),
    ("P004", "PARTY BUNTING", 4.95),
    ("P005", "LUNCH BAG RED RETROSPOT", 1.65),
    ("P006", "ASSORTED COLOUR BIRD ORNAMENT", 1.69),
    ("P007", "SET OF 3 CAKE TINS PANTRY DESIGN", 4.95),
    ("P008", "PACK OF 72 RETROSPOT CAKE CASES", 0.55),
    ("P009", "NATURAL SLATE HEART CHALKBOARD", 2.95),
    ("P010", "HEART OF WICKER SMALL", 1.65),
    ("P011", "PAPER CHAIN KIT 50'S CHRISTMAS", 2.95),
    ("P012", "VICTORIAN GLASS HANGING T-LIGHT", 1.25),
    ("P013", "SPOTTY BUNTING", 4.95),
    ("P014", "WOODEN PICTURE FRAME WHITE FINISH", 2.55),
    ("P015", "ALARM CLOCK BAKELIKE RED", 3.75),
    ("P016", "RED WOOLLY HOTTIE WHITE HEART", 3.75),
    ("P017", "HAND WARMER UNION JACK", 2.10),
    ("P018", "CHILLI LIGHTS", 4.95),
    ("P019", "DOORMAT KEEP CALM AND COME IN", 7.95),
    ("P020", "RABBIT NIGHT LIGHT", 1.79),
    ("POST", "POSTAGE", 18.00),
]
COUNTRIES = ["United Kingdom"] * 8 + ["France", "Germany"]


def generate(rows: int, seed: int, start: datetime, days: int) -> list[dict]:
    rng = random.Random(seed)
    customers = [f"C{n:03d}" for n in range(1, 61)]
    # Popularité inégale : quelques produits concentrent les ventes.
    weights = [1 / (i + 1) ** 0.8 for i in range(len(PRODUCTS))]
    out, invoice = [], 0
    while len(out) < rows:
        invoice += 1
        moment = start + timedelta(days=rng.randrange(days), hours=rng.randint(8, 18), minutes=rng.randrange(60))
        customer = "" if rng.random() < 0.2 else rng.choice(customers)
        country = rng.choice(COUNTRIES)
        for _ in range(rng.randint(1, 4)):
            pid, desc, base = rng.choices(PRODUCTS, weights)[0]
            price = round(base * rng.choice([1.0, 1.0, 1.0, 0.85, 1.15]), 2)
            qty = rng.choice([1, 1, 2, 3, 4, 6, 12, 24])
            out.append({
                "transaction_id": f"T{invoice:05d}",
                "timestamp": moment.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": customer,
                "product_id": pid,
                "description": desc,
                "quantity": qty,
                "unit_price": price,
                "total_amount": round(qty * price, 2),
                "country": country,
            })
            if len(out) == rows:
                break
    return out


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--out", type=Path, default=Path("data/sample/transactions_sample.csv"))
    args = parser.parse_args(argv)

    rows = generate(args.rows, args.seed, datetime.fromisoformat(args.start), args.days)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} lignes écrites dans {args.out}")


if __name__ == "__main__":
    main()
