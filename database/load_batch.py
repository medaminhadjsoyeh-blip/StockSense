"""Charge les résultats batch dans MongoDB (idempotent, sans fenêtre vide).

Sources :
- results/*.json                : sorties MapReduce exportées (Étudiant 1) ;
- results/data_quality_report.json ;
- CSV nettoyé (optionnel)       : sales_daily + référentiel products ;
- inventory.csv (optionnel)     : stock initial + estimation de couverture
                                  + alertes LOW_STOCK.

Usage :
    python -m database.load_batch
    python -m database.load_batch --transactions data/processed/retail_clean.csv \\
                                  --inventory data/inventory.csv
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timezone
from pathlib import Path

import pandas as pd
from pymongo.database import Database

from . import metrics, schema
from .client import get_client, get_database, ping
from .config import get_settings
from .repository import ensure_schema, record_run, sync_collection, utcnow

BATCH_FILES = {
    schema.PRODUCT_COUNT.name: "product_count.json",
    schema.PRODUCT_REVENUE.name: "product_revenue.json",
    schema.CUSTOMER_TOTAL.name: "customer_total.json",
    schema.AVERAGE_PRICE.name: "average_price.json",
    schema.TOP10.name: "top10.json",
}
QUALITY_FILE = "data_quality_report.json"
CHUNK_ROWS = 200_000


def _read_json(path: Path):
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_mapreduce_results(db: Database, results_dir: Path) -> dict[str, int]:
    counts = {}
    for collection, filename in BATCH_FILES.items():
        path = results_dir / filename
        if not path.exists():
            print(f"  ! {path} absent, collection {collection} ignorée")
            continue
        documents = _read_json(path)
        stats = sync_collection(db, collection, documents)
        counts[collection] = len(documents)
        print(f"  {collection:<20} {len(documents):>7} docs (supprimés : {stats['deleted']})")

    quality = results_dir / QUALITY_FILE
    if quality.exists():
        report = _read_json(quality)
        report.setdefault("source", "UCI Online Retail")
        sync_collection(db, schema.DATA_QUALITY_REPORT.name, [report])
        counts[schema.DATA_QUALITY_REPORT.name] = 1
        print(f"  {schema.DATA_QUALITY_REPORT.name:<20} {1:>7} doc")
    return counts


def build_daily_and_products(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lit le CSV par blocs pour limiter la mémoire ; combine des sommes."""
    daily_parts, description_parts = [], []
    for chunk in metrics.read_transactions(csv_path, chunksize=CHUNK_ROWS):
        daily_parts.append(metrics.aggregate_daily_sales(chunk))
        description_parts.append(
            chunk.groupby(["product_id", "description"]).size().reset_index(name="n")
        )
    daily = metrics.combine_daily(daily_parts)
    products = (
        metrics.combine_descriptions(description_parts)
        if description_parts
        else pd.DataFrame(columns=["product_id", "description"])
    )
    return daily, products


def _daily_documents(daily: pd.DataFrame):
    for row in daily.itertuples(index=False):
        yield {
            "date": row.date.to_pydatetime().replace(tzinfo=timezone.utc),
            "product_id": row.product_id,
            "transaction_count": int(row.transaction_count),
            "quantity": int(row.quantity),
            "revenue": round(float(row.revenue), 2),
            "unit_price_sum": round(float(row.unit_price_sum), 6),
        }


def load_transactions(db: Database, csv_path: Path) -> tuple[dict[str, int], pd.DataFrame]:
    daily, products = build_daily_and_products(csv_path)
    stats = sync_collection(db, schema.SALES_DAILY.name, _daily_documents(daily))
    print(f"  {schema.SALES_DAILY.name:<20} {len(daily):>7} docs (supprimés : {stats['deleted']})")
    sync_collection(db, schema.PRODUCTS.name, products.to_dict("records"))
    print(f"  {schema.PRODUCTS.name:<20} {len(products):>7} docs")
    return {schema.SALES_DAILY.name: len(daily), schema.PRODUCTS.name: len(products)}, daily


def read_inventory(path: Path) -> pd.DataFrame:
    inventory = pd.read_csv(path, dtype={"product_id": str})
    expected = {"product_id", "initial_stock", "reorder_threshold"}
    missing = expected - set(inventory.columns)
    if missing:
        raise ValueError(f"{path} : colonnes manquantes {sorted(missing)}")
    if inventory["product_id"].duplicated().any():
        raise ValueError(f"{path} : product_id dupliqués")
    if (inventory[["initial_stock", "reorder_threshold"]] < 0).any().any():
        raise ValueError(f"{path} : valeurs de stock négatives")
    return inventory


def estimate_stock(inventory: pd.DataFrame, daily: pd.DataFrame | None) -> tuple[list[dict], list[dict]]:
    """Estimation batch : stock courant = stock initial, vente moyenne = historique.

    Le streaming (Étudiant 2) met ensuite à jour current_stock en temps réel
    via le même contrat (source = "streaming").
    """
    now = utcnow()
    averages = metrics.average_daily_sales(daily) if daily is not None else pd.Series(dtype=float)
    items, alerts = [], []
    for row in inventory.itertuples(index=False):
        avg = float(averages.get(row.product_id, 0.0))
        current = int(row.initial_stock)
        coverage = metrics.stock_coverage_days(current, avg)
        items.append({
            "product_id": row.product_id,
            "initial_stock": int(row.initial_stock),
            "reorder_threshold": int(row.reorder_threshold),
            "current_stock": current,
            "avg_daily_sales": round(avg, 4),
            "coverage_days": None if coverage is None else round(coverage, 2),
            "source": "batch",
            "updated_at": now,
        })
        if metrics.is_low_stock(current, row.reorder_threshold, coverage):
            reason = (
                f"Couverture {coverage:.1f} j < {metrics.COVERAGE_ALERT_DAYS:g} j"
                if coverage is not None and coverage < metrics.COVERAGE_ALERT_DAYS
                else f"Stock {current} <= seuil {int(row.reorder_threshold)}"
            )
            alerts.append({
                "alert_id": f"LOW_STOCK:{row.product_id}",
                "type": "LOW_STOCK",
                # Rupture ou moins d'un jour de vente : critique ; sinon avertissement.
                "severity": "critical" if current == 0 or (coverage is not None and coverage < 1) else "warning",
                "product_id": row.product_id,
                "transaction_id": None,
                "reason": reason,
                "value": None if coverage is None else round(coverage, 2),
                "threshold": metrics.COVERAGE_ALERT_DAYS,
                "detected_at": now,
                "source": "batch",
                "status": "open",
            })
    return items, alerts


def load_inventory(db: Database, path: Path, daily: pd.DataFrame | None) -> dict[str, int]:
    items, alerts = estimate_stock(read_inventory(path), daily)
    # inventory.csv définit l'univers de produits suivis : remplacement complet.
    sync_collection(db, schema.INVENTORY.name, items)
    # Seules les alertes batch LOW_STOCK sont remplacées ; celles du streaming restent.
    sync_collection(db, schema.ALERTS.name, alerts, scope={"source": "batch", "type": "LOW_STOCK"})
    print(f"  {schema.INVENTORY.name:<20} {len(items):>7} docs")
    print(f"  {'alerts (LOW_STOCK)':<20} {len(alerts):>7} docs")
    return {schema.INVENTORY.name: len(items), "low_stock_alerts": len(alerts)}


def run(db: Database, results_dir: Path, transactions: Path | None, inventory: Path | None) -> dict[str, int]:
    ensure_schema(db)
    print("Résultats MapReduce :")
    counts = load_mapreduce_results(db, results_dir)
    daily = None
    if transactions:
        print("Transactions nettoyées :")
        more, daily = load_transactions(db, transactions)
        counts.update(more)
    if inventory:
        print("Inventaire :")
        counts.update(load_inventory(db, inventory, daily))
    record_run(db, "load_batch", counts)
    return counts


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, default=Path("results"), help="dossier des JSON MapReduce")
    parser.add_argument("--transactions", type=Path, help="CSV nettoyé (retail_clean.csv)")
    parser.add_argument("--inventory", type=Path, help="inventory.csv (product_id, initial_stock, reorder_threshold)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    for path in (args.transactions, args.inventory):
        if path and not path.exists():
            print(f"Fichier introuvable : {path}", file=sys.stderr)
            return 2
    settings = get_settings()
    client = get_client(settings)
    if not ping(client):
        print(f"MongoDB injoignable : {settings.mongo_uri} (docker compose up -d ?)", file=sys.stderr)
        return 1
    counts = run(get_database(client, settings), args.results, args.transactions, args.inventory)
    print(f"\nChargement terminé : {sum(counts.values())} documents écrits.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
