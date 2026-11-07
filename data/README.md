# Données

## Dataset principal : UCI Online Retail

- Source : <https://archive.ics.uci.edu/dataset/352/online+retail>
- Fichier attendu : `data/raw/online_retail.xlsx` (non versionné, voir `.gitignore`)
- Nettoyage : `python scripts/clean_data.py` → `data/processed/retail_clean.csv`

## Format commun des transactions (CSV nettoyé)

| Champ | Type | Exemple | Description |
|---|---|---|---|
| `transaction_id` | chaîne | `536365` | Numéro de facture (plusieurs lignes par facture) |
| `timestamp` | `YYYY-MM-DD HH:MM:SS` | `2010-12-01 08:26:00` | Date de vente, sans fuseau (traitée comme UTC) |
| `customer_id` | chaîne, vide si inconnu | `17850` | Client pseudonymisé |
| `product_id` | chaîne | `85123A` | Référence produit |
| `description` | chaîne | `WHITE HANGING HEART T-LIGHT HOLDER` | Libellé (`UNKNOWN` si absent) |
| `quantity` | entier > 0 | `6` | Quantité vendue |
| `unit_price` | décimal > 0 | `2.55` | Prix unitaire (GBP) |
| `total_amount` | décimal | `15.30` | `quantity × unit_price`, arrondi à 2 décimales |
| `country` | chaîne | `United Kingdom` | Pays du client |

## Inventaire (`inventory.csv`)

| Champ | Type | Description |
|---|---|---|
| `product_id` | chaîne | Référence produit (unique) |
| `initial_stock` | entier ≥ 0 | Stock au démarrage de la simulation |
| `reorder_threshold` | entier ≥ 0 | Seuil de réapprovisionnement |

Les transactions ne donnent pas le stock : il est **simulé** de façon
reproductible par `generate_inventory.py` (graine fixe, règles dans l'en-tête
du script).

## Échantillon synthétique (`sample/`)

Versionné, déterministe, au même format que le CSV nettoyé : sert aux tests
et à la démonstration sans le dataset.

| Fichier | Génération |
|---|---|
| `sample/transactions_sample.csv` | `python data/generate_sample.py` (3 000 lignes, 21 produits dont `POST`, 60 clients, 90 jours) |
| `sample/inventory_sample.csv` | `python data/generate_inventory.py --transactions data/sample/transactions_sample.csv --out data/sample/inventory_sample.csv` |
