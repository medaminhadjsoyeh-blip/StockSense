# StockSense – Étudiant 3
## Modèle MongoDB, accès aux données et contrat d'interface

Ce document est le **contrat commun** entre les trois modules : le batch
(Étudiant 1) et le streaming (Étudiant 2) écrivent dans MongoDB, le dashboard
(Étudiant 3) lit. La source de vérité exécutable est
[`database/schema.py`](../database/schema.py) : collections, clés, champs
obligatoires, index et validateurs y sont déclarés une seule fois.

## 1. Conventions communes

| Élément | Convention |
|---|---|
| Base | `stocksense` (variable `MONGO_DB`) |
| Identifiants | `product_id`, `customer_id`, `transaction_id` : **chaînes** (`"85123A"`, `"12346"`) |
| Montants | livres sterling (**GBP**, dataset UCI britannique), arrondis à 2 décimales |
| Prix moyen | GBP, arrondi à 4 décimales (export MapReduce) |
| Quantités | unités vendues (entiers) |
| Dates | **BSON Date en UTC**. Les dates UCI n'ont pas de fuseau : elles sont interprétées comme UTC |
| Jour de vente | `sales_daily.date` = minuit UTC du jour |
| Ligne de vente | une ligne du CSV nettoyé (une facture contient plusieurs lignes). `transaction_count` compte des **lignes**, comme le job `product_count` |
| Produit global | `product_id = "ALL"` dans `sales_windows` pour le total toutes références |

## 2. Collections

### 2.1 Résultats batch (écrits par `database/load_batch.py` depuis les JSON MapReduce)

| Collection | Clé unique | Champs | Index secondaires |
|---|---|---|---|
| `product_count` | `product_id` | `transaction_count`, `total_quantity` | – |
| `product_revenue` | `product_id` | `total_revenue` | `total_revenue` ↓ |
| `customer_total` | `customer_id` | `total_spent` | `total_spent` ↓ |
| `average_price` | `product_id` | `average_price` | – |
| `top10` | `rank` | `product_id`, `total_revenue` | – |
| `data_quality_report` | `source` | `initial_rows`, `final_rows`, `removed_rows`, `total_revenue`, `unique_products`, `unique_customers`, … | – |

Les noms et types sont **ceux produits par `scripts/export_to_json.py`**
(Étudiant 1) : aucun renommage, le dashboard lit le format existant.

### 2.2 Collections d'intégration (Étudiant 3)

| Collection | Clé unique | Champs | Rôle |
|---|---|---|---|
| `sales_daily` | (`date`, `product_id`) | `transaction_count`, `quantity`, `revenue`, `unit_price_sum` | Base des **filtres par période et produit** |
| `products` | `product_id` | `description` | Libellés lisibles dans le dashboard |
| `pipeline_runs` | `job` | `finished_at`, `counts` | Fraîcheur des données |

**Pourquoi `sales_daily` ?** Les jobs MapReduce agrègent sur toute la
période : ils ne permettent pas de filtrer par date. `sales_daily` garde la
dimension temps à la granularité du jour (≈ 1 document par produit et par jour
de vente) : assez fin pour les filtres, sans stocker les 530 000 lignes.

**Pourquoi `unit_price_sum` ?** Le prix moyen d'un produit est la moyenne
arithmétique de ses prix unitaires (job `average_price`). Pour le recalculer
sur une période quelconque, on stocke la somme des prix et le nombre de
lignes, puis on divise **après** addition :
`prix_moyen = Σ unit_price_sum / Σ transaction_count`. On ne fait jamais la
moyenne de moyennes journalières (cahier des charges §4). Sur la période
complète, le résultat est identique à `average_price` (contrôle automatique).

### 2.3 Stock et streaming (écrits par l'Étudiant 2, lus par le dashboard)

| Collection | Clé unique | Champs obligatoires | Optionnels |
|---|---|---|---|
| `inventory` | `product_id` | `initial_stock`, `reorder_threshold`, `current_stock`, `updated_at`, `source` | `avg_daily_sales`, `coverage_days` |
| `sales_windows` | (`window_start`, `product_id`) | `window_end`, `transaction_count`, `quantity`, `revenue` | `updated_at` |
| `alerts` | `alert_id` | `type`, `severity`, `reason`, `detected_at`, `source` | `product_id`, `transaction_id`, `value`, `threshold`, `status` |

Valeurs autorisées :
- `alerts.type` : `QUANTITY_ABOVE_THRESHOLD`, `PRICE_INVALID`, `PRICE_DEVIATION`,
  `DUPLICATE_TRANSACTION`, `LOW_STOCK` ;
- `alerts.severity` : `info`, `warning`, `critical` ;
- `source` : `batch` ou `streaming`.

`alert_id` doit être **stable** et déterministe, par exemple
`"QUANTITY_ABOVE_THRESHOLD:T00042"` : un événement rejoué par Kafka met à jour
l'alerte existante au lieu d'en créer une seconde.

`reason` est le **motif lisible** affiché dans le dashboard
(ex. `"quantité 500 > seuil 100"`).

`coverage_days = current_stock / avg_daily_sales` ; `null` si le produit ne
se vend pas. Seuil d'alerte : 3 jours, ou `current_stock <= reorder_threshold`.
Sévérité des alertes `LOW_STOCK` : `critical` en rupture ou sous 1 jour de
couverture, `warning` sinon.

## 3. Intégrité

- **Validateurs `$jsonSchema`** sur chaque collection (champs obligatoires et
  types BSON), `validationLevel: moderate`, `validationAction: error` : un
  document mal formé est refusé à l'écriture.
- **Index unique sur la clé métier** de chaque collection (`uniq_<champs>`) :
  pas de doublon possible, même en cas d'écritures concurrentes.
- **Écritures idempotentes** : toutes les écritures sont des upserts sur la
  clé. Rejouer un chargement donne exactement le même état (testé).
- **Remplacement sans fenêtre vide** (`sync_collection`) : la nouvelle version
  est écrite par upsert avec un identifiant de chargement (`_load_id`), puis
  les documents de l'ancienne version sont supprimés. Le dashboard ne voit
  jamais une collection vide pendant un rechargement (contrairement à
  `delete_many` puis `insert_many`).
- **Périmètre de remplacement** : le chargement batch ne remplace que les
  alertes `{source: "batch", type: "LOW_STOCK"}` ; les alertes du streaming
  sont conservées (testé).

## 4. API d'accès (`database/repository.py`)

### Écriture

| Fonction | Usage |
|---|---|
| `ensure_schema(db)` | Crée collections, validateurs et index (idempotent) |
| `upsert_many(db, collection, docs)` | Insertion / mise à jour par clé métier, par lots de 5 000 |
| `sync_collection(db, collection, docs, scope=None)` | Remplacement complet (ou limité à `scope`) sans fenêtre vide |
| `upsert_alerts(db, alerts)` | Publication d'alertes, avec contrôle du type et de la sévérité |
| `record_run(db, job, counts)` | Trace d'exécution d'un chargement |

### Lecture

| Fonction | Retour |
|---|---|
| `sales_totals(db, start, end, product_ids, exclude_ids)` | CA, quantité, lignes, produits et jours actifs sur `[start, end[` |
| `sales_by_day(...)` | Série journalière |
| `sales_by_product(..., sort_by, limit)` | Agrégats par produit (Top N), avec prix moyen recalculé |
| `get_top10`, `get_product_revenue`, `get_product_stats` | Résultats MapReduce produits |
| `get_customers(db, limit)` | Clients triés par montant |
| `get_inventory(db, product_ids, low_only)` | Stock trié par couverture croissante |
| `get_alerts(db, start, end, product_ids, types)` | Alertes récentes d'abord |
| `get_sales_windows(db, since, product_id)` | Fenêtres de streaming |
| `get_quality_report`, `get_pipeline_runs`, `collection_counts` | Métadonnées |

Toutes les agrégations sont exécutées **côté serveur** (`$match` puis
`$group`) : le dashboard reçoit des résultats agrégés, pas des documents bruts.

### Exemple pour le streaming (Étudiant 2)

```python
from database.client import get_database
from database.repository import upsert_alerts, upsert_many, utcnow

db = get_database()
upsert_alerts(db, [{
    "alert_id": f"QUANTITY_ABOVE_THRESHOLD:{event['transaction_id']}",
    "type": "QUANTITY_ABOVE_THRESHOLD",
    "severity": "critical",
    "product_id": event["product_id"],
    "transaction_id": event["transaction_id"],
    "reason": f"quantité {event['quantity']} > seuil 100",
    "value": event["quantity"],
    "threshold": 100,
    "detected_at": utcnow(),
    "source": "streaming",
}])
```

Depuis Spark, appeler ces fonctions dans `foreachBatch` (après `collect()`
d'un micro-lot agrégé) ou écrire avec le connecteur MongoDB Spark en
respectant les mêmes noms de champs et la même clé d'upsert.

## 5. Contrôles de cohérence (`database/verify.py`)

| Contrôle | Comparaison |
|---|---|
| CA total MapReduce = rapport qualité | Σ `product_revenue` vs `total_revenue` du nettoyage |
| Nombre de produits / clients | `product_revenue`, `customer_total` vs rapport qualité |
| Top 10 | `top10` vs 10 premiers de `product_revenue` |
| CA et lignes `sales_daily` | vs rapport qualité |
| CA par produit | `sales_daily` (pandas) vs `product_revenue` (MapReduce), écart ≤ 0,01 £ |
| Lignes et quantités par produit | `sales_daily` vs `product_count`, écart nul |
| Prix moyen | `Σ unit_price_sum / Σ transaction_count` vs `average_price`, écart ≤ 1e-4 |
| Recalcul CSV (`--transactions`) | module `csv` + `Decimal`, indépendant de pandas et de MapReduce |

Les mêmes contrôles sont affichés dans l'onglet *Qualité & cohérence* du
dashboard.

## 6. Limites connues

- `sales_daily` est calculé avec pandas dans `load_batch.py`. Un sixième job
  MapReduce de clé `(date, product_id)` pourrait le produire dans Hadoop ;
  le format de sortie resterait le même.
- L'inventaire du dataset UCI est **simulé** (`data/generate_inventory.py`) :
  les transactions ne contiennent ni stock initial ni réapprovisionnement.
- La couverture de stock est une estimation : elle suppose que le rythme de
  vente des 30 derniers jours se maintient.
- Les dates sont traitées comme UTC faute d'information de fuseau dans le
  dataset source.
