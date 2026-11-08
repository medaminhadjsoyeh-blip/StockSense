"""Fonctions d'insertion, de mise à jour et de lecture MongoDB.

Principes :
- Toutes les écritures sont des upserts sur la clé métier déclarée dans
  schema.py : rejouer un chargement ne crée jamais de doublon.
- sync_collection remplace le contenu d'une collection sans fenêtre vide :
  upsert de la nouvelle version, puis suppression des documents absents.
- Les lectures renvoient des listes de dict sans _id, prêtes pour pandas.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from itertools import islice

from pymongo import ASCENDING, DESCENDING, UpdateOne
from pymongo.database import Database
from pymongo.errors import CollectionInvalid, OperationFailure

from . import schema
from .schema import ALERT_SEVERITIES, ALERT_TYPES, BY_NAME, CollectionSpec

LOAD_ID = "_load_id"
BATCH_SIZE = 5000


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _spec(name: str) -> CollectionSpec:
    try:
        return BY_NAME[name]
    except KeyError:
        raise ValueError(f"Collection inconnue du contrat : {name}") from None


def _chunks(items: Iterable, size: int) -> Iterator[list]:
    iterator = iter(items)
    while chunk := list(islice(iterator, size)):
        yield chunk


# --- Schéma -----------------------------------------------------------------

def key_index_name(spec: CollectionSpec) -> str:
    return "uniq_" + "_".join(spec.key)


def ensure_schema(db: Database, apply_validators: bool = True) -> dict[str, list[str]]:
    """Crée collections, validateurs et index. Idempotent.

    validationLevel=moderate : les documents existants non conformes ne
    bloquent pas les mises à jour ; les nouvelles écritures sont contrôlées.
    """
    report: dict[str, list[str]] = {}
    existing = set(db.list_collection_names())
    for spec in schema.ALL_COLLECTIONS:
        if apply_validators:
            options = {
                "validator": spec.validator(),
                "validationLevel": "moderate",
                "validationAction": "error",
            }
            if spec.name in existing:
                db.command("collMod", spec.name, **options)
            else:
                try:
                    db.create_collection(spec.name, **options)
                except CollectionInvalid:
                    db.command("collMod", spec.name, **options)
        collection = db[spec.name]
        names = [
            collection.create_index(
                [(field, ASCENDING) for field in spec.key], unique=True, name=key_index_name(spec)
            )
        ]
        for index in spec.indexes:
            names.append(collection.create_index(list(index)))
        report[spec.name] = names
    return report


# --- Écriture -----------------------------------------------------------------

def upsert_many(
    db: Database,
    collection: str,
    documents: Iterable[dict],
    extra: dict | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    """Insère ou met à jour des documents selon la clé métier du contrat."""
    spec = _spec(collection)
    stats = {"matched": 0, "upserted": 0, "modified": 0}
    for chunk in _chunks(documents, batch_size):
        operations = []
        for document in chunk:
            missing = [field for field in spec.key if document.get(field) is None]
            if missing:
                raise ValueError(f"{collection} : clé manquante {missing} dans {document}")
            payload = {**document, **(extra or {})}
            payload.pop("_id", None)
            key = {field: document[field] for field in spec.key}
            operations.append(UpdateOne(key, {"$set": payload}, upsert=True))
        result = db[collection].bulk_write(operations, ordered=False)
        stats["matched"] += result.matched_count
        stats["upserted"] += result.upserted_count
        stats["modified"] += result.modified_count
    return stats


def sync_collection(
    db: Database,
    collection: str,
    documents: Iterable[dict],
    scope: dict | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    """Remplace le contenu (limité à scope) par documents, sans période vide.

    Les documents non réécrits par ce chargement sont supprimés à la fin.
    scope permet de ne remplacer qu'une partie (ex. alertes batch uniquement).
    """
    load_id = uuid.uuid4().hex
    stats = upsert_many(db, collection, documents, extra={LOAD_ID: load_id}, batch_size=batch_size)
    stale = {**(scope or {}), LOAD_ID: {"$ne": load_id}}
    stats["deleted"] = db[collection].delete_many(stale).deleted_count
    return stats


def upsert_alerts(db: Database, alerts: Iterable[dict]) -> dict[str, int]:
    """Point d'entrée du streaming pour publier des alertes.

    alert_id doit être stable (ex. "QUANTITY_ABOVE_THRESHOLD:T00042") pour
    qu'un événement rejoué ne crée pas de seconde alerte.
    """
    checked = []
    for alert in alerts:
        if alert.get("type") not in ALERT_TYPES:
            raise ValueError(f"Type d'alerte inconnu : {alert.get('type')} (attendu {ALERT_TYPES})")
        if alert.get("severity") not in ALERT_SEVERITIES:
            raise ValueError(f"Sévérité inconnue : {alert.get('severity')}")
        checked.append({"status": "open", **alert})
    return upsert_many(db, schema.ALERTS.name, checked)


def record_run(db: Database, job: str, counts: dict[str, int] | None = None) -> None:
    db[schema.PIPELINE_RUNS.name].update_one(
        {"job": job},
        {"$set": {"job": job, "finished_at": utcnow(), "counts": counts or {}}},
        upsert=True,
    )


# --- Lecture : métadonnées -------------------------------------------------

_NO_ID = {"_id": 0, LOAD_ID: 0}


def collection_counts(db: Database) -> dict[str, int]:
    return {spec.name: db[spec.name].count_documents({}) for spec in schema.ALL_COLLECTIONS}


def get_quality_report(db: Database) -> dict | None:
    return db[schema.DATA_QUALITY_REPORT.name].find_one({}, _NO_ID)


def get_pipeline_runs(db: Database) -> list[dict]:
    return list(db[schema.PIPELINE_RUNS.name].find({}, _NO_ID).sort("job", ASCENDING))


def list_products(db: Database) -> list[dict]:
    """Produits connus (référentiel, sinon résultats MapReduce)."""
    products = list(db[schema.PRODUCTS.name].find({}, _NO_ID).sort("product_id", ASCENDING))
    if products:
        return products
    ids = db[schema.PRODUCT_REVENUE.name].distinct("product_id")
    return [{"product_id": pid, "description": ""} for pid in sorted(ids)]


def sales_date_bounds(db: Database) -> tuple[datetime, datetime] | None:
    collection = db[schema.SALES_DAILY.name]
    first = collection.find_one({}, {"date": 1}, sort=[("date", ASCENDING)])
    last = collection.find_one({}, {"date": 1}, sort=[("date", DESCENDING)])
    if not first or not last:
        return None
    return first["date"], last["date"]


# --- Lecture : ventes par période ----------------------------------------------

def _sales_match(start, end, product_ids=None, exclude_ids=None) -> dict:
    """Filtre sur sales_daily : [start, end[ (end exclusif), produits inclus/exclus."""
    match: dict = {}
    if start or end:
        match["date"] = {}
        if start:
            match["date"]["$gte"] = start
        if end:
            match["date"]["$lt"] = end
    if product_ids:
        match.setdefault("product_id", {})["$in"] = list(product_ids)
    if exclude_ids:
        match.setdefault("product_id", {})["$nin"] = list(exclude_ids)
    return match


_SUMS = {
    "transaction_count": {"$sum": "$transaction_count"},
    "quantity": {"$sum": "$quantity"},
    "revenue": {"$sum": "$revenue"},
    "unit_price_sum": {"$sum": "$unit_price_sum"},
}


def sales_totals(db: Database, start=None, end=None, product_ids=None, exclude_ids=None) -> dict:
    """Totaux sur la période : CA, quantité, lignes, produits actifs."""
    pipeline = [
        {"$match": _sales_match(start, end, product_ids, exclude_ids)},
        {"$group": {"_id": None, **_SUMS, "products": {"$addToSet": "$product_id"}, "days": {"$addToSet": "$date"}}},
    ]
    rows = list(db[schema.SALES_DAILY.name].aggregate(pipeline))
    if not rows:
        return {"transaction_count": 0, "quantity": 0, "revenue": 0.0, "unit_price_sum": 0.0, "products": 0, "days": 0}
    row = rows[0]
    row.pop("_id")
    row["products"] = len(row["products"])
    row["days"] = len(row["days"])
    return row


def sales_by_day(db: Database, start=None, end=None, product_ids=None, exclude_ids=None) -> list[dict]:
    """Une ligne par jour avec ventes (sommes sur les produits filtrés)."""
    pipeline = [
        {"$match": _sales_match(start, end, product_ids, exclude_ids)},
        {"$group": {"_id": "$date", **_SUMS}},
        {"$sort": {"_id": 1}},
    ]
    return [
        {"date": row.pop("_id"), **row}
        for row in db[schema.SALES_DAILY.name].aggregate(pipeline)
    ]


def sales_by_product(
    db: Database,
    start=None,
    end=None,
    product_ids=None,
    sort_by: str = "revenue",
    limit: int | None = None,
    exclude_ids: list[str] | None = None,
) -> list[dict]:
    """Agrégats par produit sur la période, triés (Top N si limit)."""
    match = _sales_match(start, end, product_ids, exclude_ids)
    pipeline: list[dict] = [
        {"$match": match},
        {"$group": {"_id": "$product_id", **_SUMS}},
        {"$sort": {sort_by: -1, "_id": 1}},
    ]
    if limit:
        pipeline.append({"$limit": int(limit)})
    rows = []
    for row in db[schema.SALES_DAILY.name].aggregate(pipeline):
        row["product_id"] = row.pop("_id")
        count = row["transaction_count"]
        row["average_price"] = row["unit_price_sum"] / count if count else None
        rows.append(row)
    return rows


# --- Lecture : résultats batch -----------------------------------------------

def get_top10(db: Database) -> list[dict]:
    return list(db[schema.TOP10.name].find({}, _NO_ID).sort("rank", ASCENDING))


def get_product_revenue(db: Database, limit: int | None = None, exclude_ids=None) -> list[dict]:
    query = {"product_id": {"$nin": list(exclude_ids)}} if exclude_ids else {}
    cursor = db[schema.PRODUCT_REVENUE.name].find(query, _NO_ID).sort(
        [("total_revenue", DESCENDING), ("product_id", ASCENDING)]
    )
    if limit:
        cursor = cursor.limit(int(limit))
    return list(cursor)


def get_product_stats(db: Database) -> list[dict]:
    """Jointure product_count + product_revenue + average_price + description."""
    pipeline = [
        {"$lookup": {"from": schema.PRODUCT_REVENUE.name, "localField": "product_id", "foreignField": "product_id", "as": "rev"}},
        {"$lookup": {"from": schema.AVERAGE_PRICE.name, "localField": "product_id", "foreignField": "product_id", "as": "avg"}},
        {"$lookup": {"from": schema.PRODUCTS.name, "localField": "product_id", "foreignField": "product_id", "as": "ref"}},
        {
            "$project": {
                "_id": 0,
                "product_id": 1,
                "transaction_count": 1,
                "total_quantity": 1,
                "total_revenue": {"$arrayElemAt": ["$rev.total_revenue", 0]},
                "average_price": {"$arrayElemAt": ["$avg.average_price", 0]},
                "description": {"$arrayElemAt": ["$ref.description", 0]},
            }
        },
    ]
    return list(db[schema.PRODUCT_COUNT.name].aggregate(pipeline))


def get_customers(db: Database, limit: int | None = None) -> list[dict]:
    cursor = db[schema.CUSTOMER_TOTAL.name].find({}, _NO_ID).sort(
        [("total_spent", DESCENDING), ("customer_id", ASCENDING)]
    )
    if limit:
        cursor = cursor.limit(int(limit))
    return list(cursor)


def get_average_prices(db: Database, product_ids=None) -> list[dict]:
    query = {"product_id": {"$in": list(product_ids)}} if product_ids else {}
    return list(db[schema.AVERAGE_PRICE.name].find(query, _NO_ID).sort("product_id", ASCENDING))


# --- Lecture : stock, alertes, streaming ----------------------------------------

def get_inventory(db: Database, product_ids=None, low_only: bool = False) -> list[dict]:
    query: dict = {}
    if product_ids:
        query["product_id"] = {"$in": list(product_ids)}
    rows = list(db[schema.INVENTORY.name].find(query, _NO_ID))
    if low_only:
        from .metrics import is_low_stock

        rows = [
            r for r in rows
            if is_low_stock(r["current_stock"], r["reorder_threshold"], r.get("coverage_days"))
        ]
    # Couverture la plus faible d'abord ; produits sans vente (None) à la fin.
    rows.sort(key=lambda r: (r.get("coverage_days") is None, r.get("coverage_days") or 0, r["product_id"]))
    return rows


def get_alerts(
    db: Database,
    start=None,
    end=None,
    product_ids=None,
    types: list[str] | None = None,
    limit: int = 500,
) -> list[dict]:
    query: dict = {}
    if start or end:
        query["detected_at"] = {}
        if start:
            query["detected_at"]["$gte"] = start
        if end:
            query["detected_at"]["$lt"] = end
    if product_ids:
        query["product_id"] = {"$in": list(product_ids)}
    if types:
        query["type"] = {"$in": list(types)}
    cursor = db[schema.ALERTS.name].find(query, _NO_ID).sort(
        [("detected_at", DESCENDING), ("alert_id", ASCENDING)]
    ).limit(int(limit))
    return list(cursor)


def get_sales_windows(db: Database, since=None, product_id: str = schema.GLOBAL_PRODUCT, limit: int = 1000) -> list[dict]:
    query: dict = {"product_id": product_id}
    if since:
        query["window_start"] = {"$gte": since}
    cursor = db[schema.SALES_WINDOWS.name].find(query, _NO_ID).sort("window_start", DESCENDING).limit(int(limit))
    return list(reversed(list(cursor)))


def drop_all(db: Database) -> None:
    """Supprime toutes les collections du contrat (tests / réinitialisation)."""
    for spec in schema.ALL_COLLECTIONS:
        try:
            db.drop_collection(spec.name)
        except OperationFailure:
            pass
