"""Contrôles de cohérence : les chiffres stockés et affichés sont-ils justes ?

Trois sources sont comparées :
1. les résultats MapReduce (product_*, customer_total, top10) ;
2. le rapport qualité du nettoyage ;
3. sales_daily (agrégé par load_batch.py avec pandas) ;
et, avec --transactions, un recalcul indépendant depuis le CSV nettoyé
avec le module csv et Decimal (aucun code partagé avec pandas ni MapReduce).

Usage :
    python -m database.verify
    python -m database.verify --transactions data/processed/retail_clean.csv
Code retour 0 si tous les contrôles exécutés passent, 1 sinon.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path

from pymongo.database import Database

from . import repository, schema
from .client import get_client, get_database, ping
from .config import get_settings

MONEY_TOL = 0.01  # 1 penny
PRICE_TOL = 1e-4  # average_price exporté arrondi à 4 décimales


@dataclass
class Check:
    name: str
    status: str  # pass | fail | skip
    expected: object = None
    actual: object = None
    detail: str = ""


def _ok(condition: bool) -> str:
    return "pass" if condition else "fail"


def _by(rows, key, field):
    return {row[key]: row[field] for row in rows}


def _max_diff(left: dict, right: dict) -> tuple[float, list]:
    """Écart maximal entre deux dict ; clés manquantes d'un côté signalées."""
    missing = sorted(set(left) ^ set(right))
    common = set(left) & set(right)
    worst = max((abs(float(left[k]) - float(right[k])) for k in common), default=0.0)
    return worst, missing


def check_database(db: Database) -> list[Check]:
    checks: list[Check] = []
    quality = repository.get_quality_report(db)
    revenue_rows = list(db[schema.PRODUCT_REVENUE.name].find({}, {"_id": 0}))
    count_rows = list(db[schema.PRODUCT_COUNT.name].find({}, {"_id": 0}))
    avg_rows = list(db[schema.AVERAGE_PRICE.name].find({}, {"_id": 0}))
    customers = db[schema.CUSTOMER_TOTAL.name].count_documents({})
    revenue = _by(revenue_rows, "product_id", "total_revenue")

    if quality:
        total = round(sum(revenue.values()), 2)
        checks.append(Check(
            "CA total MapReduce = rapport qualité",
            _ok(abs(total - quality["total_revenue"]) <= MONEY_TOL),
            quality["total_revenue"], total,
        ))
        checks.append(Check(
            "Nombre de produits = rapport qualité",
            _ok(len(revenue) == quality.get("unique_products")),
            quality.get("unique_products"), len(revenue),
        ))
        checks.append(Check(
            "Nombre de clients = rapport qualité",
            _ok(customers == quality.get("unique_customers")),
            quality.get("unique_customers"), customers,
        ))
    else:
        checks.append(Check("Rapport qualité présent", "fail", "1 document", "0"))

    top10 = repository.get_top10(db)
    expected_top = [r["product_id"] for r in repository.get_product_revenue(db, limit=10)]
    actual_top = [r["product_id"] for r in top10]
    checks.append(Check(
        "Top 10 = 10 premiers de product_revenue",
        _ok(bool(top10) and expected_top == actual_top),
        expected_top, actual_top,
    ))

    daily = repository.sales_by_product(db)
    if not daily:
        checks.append(Check("sales_daily cohérent avec MapReduce", "skip",
                            detail="sales_daily vide : charger --transactions"))
        return checks

    totals = repository.sales_totals(db)
    if quality:
        checks.append(Check(
            "CA sales_daily = rapport qualité",
            _ok(abs(totals["revenue"] - quality["total_revenue"]) <= MONEY_TOL),
            quality["total_revenue"], round(totals["revenue"], 2),
        ))
        checks.append(Check(
            "Lignes sales_daily = lignes nettoyées",
            _ok(totals["transaction_count"] == quality["final_rows"]),
            quality["final_rows"], totals["transaction_count"],
        ))

    worst, missing = _max_diff(_by(daily, "product_id", "revenue"), revenue)
    checks.append(Check(
        "CA par produit : sales_daily = product_revenue",
        _ok(worst <= MONEY_TOL and not missing),
        f"écart <= {MONEY_TOL}", round(worst, 6),
        f"produits non appariés : {missing[:5]}" if missing else "",
    ))

    counts = _by(count_rows, "product_id", "transaction_count")
    quantities = _by(count_rows, "product_id", "total_quantity")
    worst_c, missing_c = _max_diff(_by(daily, "product_id", "transaction_count"), counts)
    worst_q, _ = _max_diff(_by(daily, "product_id", "quantity"), quantities)
    checks.append(Check(
        "Ventes et quantités par produit = product_count",
        _ok(worst_c == 0 and worst_q == 0 and not missing_c),
        "écart 0", f"lignes {worst_c:g}, quantité {worst_q:g}",
    ))

    worst_p, missing_p = _max_diff(
        _by(daily, "product_id", "average_price"), _by(avg_rows, "product_id", "average_price")
    )
    checks.append(Check(
        "Prix moyen recalculé (somme/nombre) = average_price",
        _ok(worst_p <= PRICE_TOL and not missing_p),
        f"écart <= {PRICE_TOL}", round(worst_p, 8),
    ))
    return checks


def recompute_from_csv(path: Path) -> dict:
    """Recalcul indépendant : csv + Decimal, sans pandas ni MapReduce."""
    revenue: dict[str, Decimal] = defaultdict(Decimal)
    customers: set[str] = set()
    rows = 0
    total = Decimal(0)
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            amount = Decimal(row["total_amount"])
            revenue[row["product_id"]] += amount
            total += amount
            rows += 1
            if row["customer_id"]:
                customers.add(row["customer_id"])
    return {"rows": rows, "total_revenue": total, "revenue": revenue, "customers": len(customers)}


def check_against_csv(db: Database, path: Path) -> list[Check]:
    ref = recompute_from_csv(path)
    totals = repository.sales_totals(db)
    revenue = _by(repository.get_product_revenue(db), "product_id", "total_revenue")
    worst, missing = _max_diff({k: float(v) for k, v in ref["revenue"].items()}, revenue)
    customers = db[schema.CUSTOMER_TOTAL.name].count_documents({})
    return [
        Check("CSV : CA total = sales_daily",
              _ok(abs(float(ref["total_revenue"]) - totals["revenue"]) <= MONEY_TOL),
              str(ref["total_revenue"]), round(totals["revenue"], 2)),
        Check("CSV : lignes = sales_daily", _ok(ref["rows"] == totals["transaction_count"]),
              ref["rows"], totals["transaction_count"]),
        Check("CSV : CA par produit = product_revenue (MapReduce)",
              _ok(worst <= MONEY_TOL and not missing), f"écart <= {MONEY_TOL}", round(worst, 6),
              f"produits non appariés : {missing[:5]}" if missing else ""),
        Check("CSV : clients distincts = customer_total", _ok(ref["customers"] == customers),
              ref["customers"], customers),
    ]


def run_checks(db: Database, transactions: Path | None = None) -> list[dict]:
    checks = check_database(db)
    if transactions:
        checks += check_against_csv(db, transactions)
    return [asdict(c) for c in checks]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Contrôles de cohérence StockSense")
    parser.add_argument("--transactions", type=Path, help="CSV nettoyé pour un recalcul indépendant")
    args = parser.parse_args(argv)

    settings = get_settings()
    client = get_client(settings)
    if not ping(client):
        print(f"MongoDB injoignable : {settings.mongo_uri}", file=sys.stderr)
        return 1
    results = run_checks(get_database(client, settings), args.transactions)
    symbols = {"pass": "OK  ", "fail": "FAIL", "skip": "SKIP"}
    for check in results:
        line = f"[{symbols[check['status']]}] {check['name']}"
        if check["status"] != "skip":
            line += f"  attendu={check['expected']}  obtenu={check['actual']}"
        if check["detail"]:
            line += f"  ({check['detail']})"
        print(line)
    failed = [c for c in results if c["status"] == "fail"]
    print(f"\n{len(results) - len(failed)}/{len(results)} contrôles sans échec.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
