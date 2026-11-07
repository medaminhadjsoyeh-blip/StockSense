"""Contrat de données MongoDB de StockSense : source unique de vérité.

Chaque collection déclare :
- key        : champs formant l'identifiant métier (index unique, clé d'upsert) ;
- required   : champs obligatoires et leur type BSON (validateur $jsonSchema) ;
- indexes    : index secondaires utilisés par le dashboard ;
- producer   : module qui écrit la collection.

Conventions communes (voir docs/mongodb_model.md) :
- identifiants (product_id, customer_id, transaction_id) : chaînes ;
- montants : livres sterling (GBP), arrondis à 2 décimales ;
- quantités : unités vendues ;
- dates : BSON Date en UTC. Les dates du dataset UCI n'ont pas de fuseau,
  elles sont interprétées comme UTC ;
- sales_daily.date : minuit UTC du jour de vente.
"""

from dataclasses import dataclass, field

from pymongo import ASCENDING, DESCENDING

NUMBER = "number"  # alias $jsonSchema : int, long, double, decimal
STRING = "string"
DATE = "date"


@dataclass(frozen=True)
class CollectionSpec:
    name: str
    key: tuple[str, ...]
    required: dict[str, str]
    producer: str
    description: str
    indexes: tuple[tuple[tuple[str, int], ...], ...] = field(default_factory=tuple)
    optional: dict[str, str | list[str]] = field(default_factory=dict)

    def validator(self) -> dict:
        properties = {name: {"bsonType": t} for name, t in self.required.items()}
        properties.update({name: {"bsonType": t} for name, t in self.optional.items()})
        return {
            "$jsonSchema": {
                "bsonType": "object",
                "required": list(self.required),
                "properties": properties,
            }
        }


# --- Résultats batch MapReduce (Étudiant 1) --------------------------------

PRODUCT_COUNT = CollectionSpec(
    name="product_count",
    key=("product_id",),
    required={"product_id": STRING, "transaction_count": NUMBER, "total_quantity": NUMBER},
    producer="MapReduce product_count",
    description="Nombre de lignes de vente et quantité totale par produit.",
)

PRODUCT_REVENUE = CollectionSpec(
    name="product_revenue",
    key=("product_id",),
    required={"product_id": STRING, "total_revenue": NUMBER},
    indexes=((("total_revenue", DESCENDING),),),
    producer="MapReduce product_revenue",
    description="Chiffre d'affaires total par produit (GBP).",
)

CUSTOMER_TOTAL = CollectionSpec(
    name="customer_total",
    key=("customer_id",),
    required={"customer_id": STRING, "total_spent": NUMBER},
    indexes=((("total_spent", DESCENDING),),),
    producer="MapReduce customer_total",
    description="Montant total dépensé par client identifié (GBP).",
)

AVERAGE_PRICE = CollectionSpec(
    name="average_price",
    key=("product_id",),
    required={"product_id": STRING, "average_price": NUMBER},
    producer="MapReduce average_price",
    description="Moyenne arithmétique des prix unitaires par produit (GBP).",
)

TOP10 = CollectionSpec(
    name="top10",
    key=("rank",),
    required={"rank": NUMBER, "product_id": STRING, "total_revenue": NUMBER},
    producer="MapReduce top10",
    description="Dix produits au plus fort chiffre d'affaires.",
)

DATA_QUALITY_REPORT = CollectionSpec(
    name="data_quality_report",
    key=("source",),
    required={"source": STRING, "initial_rows": NUMBER, "final_rows": NUMBER, "total_revenue": NUMBER},
    producer="scripts/clean_data.py",
    description="Rapport qualité du nettoyage (un document par source).",
)

# --- Collections d'intégration (Étudiant 3) --------------------------------

PRODUCTS = CollectionSpec(
    name="products",
    key=("product_id",),
    required={"product_id": STRING, "description": STRING},
    producer="database/load_batch.py",
    description="Référentiel produit : description la plus fréquente par product_id.",
)

SALES_DAILY = CollectionSpec(
    name="sales_daily",
    key=("date", "product_id"),
    required={
        "date": DATE,
        "product_id": STRING,
        "transaction_count": NUMBER,
        "quantity": NUMBER,
        "revenue": NUMBER,
    },
    indexes=((("product_id", ASCENDING), ("date", ASCENDING)),),
    producer="database/load_batch.py",
    description="Ventes agrégées par jour et par produit : base des filtres par période.",
)

PIPELINE_RUNS = CollectionSpec(
    name="pipeline_runs",
    key=("job",),
    required={"job": STRING, "finished_at": DATE},
    optional={"counts": "object"},
    producer="database/load_batch.py, streaming",
    description="Dernière exécution de chaque chargement (fraîcheur des données).",
)

# --- Streaming et stock (écrit par Étudiant 2, lu par le dashboard) --------

INVENTORY = CollectionSpec(
    name="inventory",
    key=("product_id",),
    required={
        "product_id": STRING,
        "initial_stock": NUMBER,
        "reorder_threshold": NUMBER,
        "current_stock": NUMBER,
        "updated_at": DATE,
        "source": STRING,
    },
    optional={"avg_daily_sales": NUMBER, "coverage_days": ["double", "int", "null"]},
    indexes=((("coverage_days", ASCENDING),),),
    producer="database/load_batch.py (estimation batch), Spark (temps réel)",
    description="État du stock et couverture estimée en jours.",
)

SALES_WINDOWS = CollectionSpec(
    name="sales_windows",
    key=("window_start", "product_id"),
    required={
        "window_start": DATE,
        "window_end": DATE,
        "product_id": STRING,
        "transaction_count": NUMBER,
        "quantity": NUMBER,
        "revenue": NUMBER,
    },
    optional={"updated_at": DATE},
    indexes=((("window_start", DESCENDING),),),
    producer="Spark Structured Streaming",
    description="Agrégats par fenêtre temporelle. product_id = 'ALL' pour le total.",
)

ALERTS = CollectionSpec(
    name="alerts",
    key=("alert_id",),
    required={
        "alert_id": STRING,
        "type": STRING,
        "severity": STRING,
        "reason": STRING,
        "detected_at": DATE,
        "source": STRING,
    },
    optional={
        "product_id": ["string", "null"],
        "transaction_id": ["string", "null"],
        "value": ["double", "int", "long", "null"],
        "threshold": ["double", "int", "long", "null"],
    },
    indexes=(
        (("detected_at", DESCENDING),),
        (("type", ASCENDING), ("detected_at", DESCENDING)),
    ),
    producer="Spark (anomalies), database/load_batch.py (stock bas)",
    description="Alertes explicables : anomalies de vente et stock bas.",
)

ALL_COLLECTIONS: tuple[CollectionSpec, ...] = (
    PRODUCT_COUNT,
    PRODUCT_REVENUE,
    CUSTOMER_TOTAL,
    AVERAGE_PRICE,
    TOP10,
    DATA_QUALITY_REPORT,
    PRODUCTS,
    SALES_DAILY,
    PIPELINE_RUNS,
    INVENTORY,
    SALES_WINDOWS,
    ALERTS,
)

BY_NAME = {spec.name: spec for spec in ALL_COLLECTIONS}

# Valeurs autorisées pour les alertes (contrat avec le streaming).
ALERT_TYPES = (
    "QUANTITY_ABOVE_THRESHOLD",
    "PRICE_INVALID",
    "PRICE_DEVIATION",
    "DUPLICATE_TRANSACTION",
    "LOW_STOCK",
)
ALERT_SEVERITIES = ("info", "warning", "critical")
GLOBAL_PRODUCT = "ALL"
