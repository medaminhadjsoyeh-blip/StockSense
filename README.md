# StockSense – Big Data Retail Analytics

Plateforme d'analyse des ventes, de surveillance des stocks et de détection
d'anomalies pour une entreprise de distribution. Projet Big Data en trinôme (II3).

Chaîne de traitement :

```
CSV brut ─► nettoyage ─► HDFS ─► MapReduce (Mapper ► Shuffle & Sort ► Combiner ► Reducer) ─┐
                                                                                         ├─► MongoDB ─► Dashboard Streamlit
Générateur ─► Kafka (topic sales) ─► Spark Structured Streaming (fenêtres, alertes) ────┘
```

## 1. Répartition du travail

| Membre | Rôle | Dossiers |
|---|---|---|
| Étudiant 1 | Data Engineering, nettoyage, HDFS, MapReduce, Combiner, export | `scripts/`, `mapreduce/`, `results/`, `docs/hadoop_mapreduce.md` |
| Étudiant 2 | Streaming Kafka / Spark, anomalies, indicateurs de stock | `streaming/` |
| Étudiant 3 | Modèle MongoDB, accès aux données, dashboard, intégration, tests, README | `database/`, `dashboard/`, `tests/`, `data/`, `docs/mongodb_model.md`, `docs/guide_utilisateur.md` |

## 2. Technologies et versions

| Outil | Version |
|---|---|
| Python | 3.10 ou plus (testé en 3.12) |
| pandas / pymongo / Streamlit / Altair | voir `requirements.txt` |
| Hadoop + Hadoop Streaming | 3.2.1 |
| MongoDB | 7.0 (Docker) |
| Docker / Docker Compose | Docker Desktop ou Docker Engine récent |

## 3. Structure du dépôt

```
StockSense/
├── data/                  générateurs d'échantillons, échantillon synthétique, README
│   ├── sample/            transactions et inventaire synthétiques (versionnés)
│   ├── raw/, processed/   dataset UCI (non versionnés)
├── scripts/               nettoyage, export JSON, import, MapReduce local
├── mapreduce/src/         mapper.py, combiner.py, reducer.py
├── results/               sorties MapReduce de référence (TSV + JSON)
├── database/              contrat MongoDB, accès, chargement, contrôles
├── dashboard/             application Streamlit
├── tests/                 tests unitaires et d'intégration
├── docs/                  documentation technique, guide utilisateur, captures
├── docker-compose.yml     MongoDB 7.0
└── requirements.txt
```

## 4. Installation

```bash
git clone https://github.com/medaminhadjsoyeh-blip/StockSense.git
cd StockSense
python -m venv .venv
# Windows : .venv\Scripts\activate    Linux / macOS : source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d          # MongoDB sur localhost:27017 (conteneur stocksense-mongodb)
```

Configuration par variables d'environnement (voir `.env.example`) :
`MONGO_URI` (défaut `mongodb://localhost:27017/`) et `MONGO_DB` (défaut `stocksense`).

## 5. Démarrage rapide

### 5.1 Démonstration sur l'échantillon synthétique (sans Hadoop ni dataset)

Les jobs MapReduce de l'Étudiant 1 sont exécutés en local, sans modification,
avec un tri entre les étapes comme le ferait Hadoop Streaming :

```bash
python scripts/local_mapreduce.py --transactions data/sample/transactions_sample.csv --root build/sample
```

```bash
MONGO_DB=stocksense_demo python -m database.load_batch --results build/sample/results --transactions data/sample/transactions_sample.csv --inventory data/sample/inventory_sample.csv
```

```bash
MONGO_DB=stocksense_demo streamlit run dashboard/app.py
```

Sous PowerShell, remplacer le préfixe `MONGO_DB=...` par `$env:MONGO_DB="stocksense_demo";`.

### 5.2 Chaîne complète sur le dataset UCI Online Retail

Ordre de lancement :

| # | Étape | Commande |
|---|---|---|
| 1 | Télécharger `online_retail.xlsx` dans `data/raw/` | voir `data/README.md` |
| 2 | Nettoyer le dataset | `python scripts/clean_data.py` |
| 3 | Charger dans HDFS et lancer les jobs MapReduce | voir `docs/hadoop_mapreduce.md` |
| 4 | Exporter les résultats en JSON | `python scripts/export_to_json.py` |
| 5 | Simuler l'inventaire | `python data/generate_inventory.py --transactions data/processed/retail_clean.csv --out data/inventory.csv` |
| 6 | Créer le schéma MongoDB (validateurs, index) | `python -m database.setup_db` |
| 7 | Charger MongoDB | `python -m database.load_batch --transactions data/processed/retail_clean.csv --inventory data/inventory.csv` |
| 8 | Vérifier la cohérence | `python -m database.verify --transactions data/processed/retail_clean.csv` |
| 9 | Lancer le streaming (Étudiant 2) | voir `streaming/` |
| 10 | Lancer le dashboard | `streamlit run dashboard/app.py` → <http://localhost:8501> |

Sans le CSV nettoyé, l'étape 7 peut se limiter à `python -m database.load_batch` :
les résultats MapReduce de `results/` sont chargés et le dashboard affiche les
totaux globaux (sans filtre de période).

`load_batch` est **idempotent** : il peut être relancé sans créer de doublons
et sans vider les collections pendant le chargement.

## 6. Résultats attendus

Sur UCI Online Retail (`python -m database.verify`) :

| Indicateur | Valeur |
|---|---:|
| Lignes initiales / après nettoyage | 541 909 / 530 104 |
| Produits distincts | 3 922 |
| Clients identifiés | 4 338 |
| Chiffre d'affaires total | 10 666 684,54 £ |
| Documents MongoDB (résultats MapReduce + rapport) | 16 115 |

Top 10 par chiffre d'affaires : `DOT`, `22423`, `23843`, `85123A`, `47566`,
`85099B`, `23166`, `M`, `POST`, `23084`. `DOT`, `M` et `POST` sont des frais
(port, ajustements) ; le dashboard propose de les exclure du classement.

Sur l'échantillon synthétique : 3 000 lignes, 21 produits, 60 clients,
87 916,57 £, 5 alertes de stock bas, 13/13 contrôles de cohérence.

## 7. Tests

```bash
python -m pytest
```

- `tests/test_metrics.py` : calculs purs et critères d'acceptation du cahier
  (comptage, somme, moyenne sans moyenne de moyennes, couverture de stock 12 / 6 = 2 jours).
- `tests/test_repository.py` : insertion, upsert idempotent, remplacement sans
  fenêtre vide, filtres de période et de produit.
- `tests/test_integration.py` : chaîne complète sur le même jeu de données,
  CSV → jobs MapReduce de l'Étudiant 1 → MongoDB → contrôles de cohérence →
  dashboard (valeurs affichées comparées à un calcul pandas indépendant).

Les tests utilisant MongoDB créent une base jetable et sont ignorés si le
serveur n'est pas joignable (`MONGO_TEST_URI` pour une autre adresse).

## 8. Partie batch (Étudiant 1)

Cinq jobs MapReduce : `product_count`, `product_revenue`, `customer_total`,
`average_price`, `top10`. Le Combiner réduit les enregistrements transmis au
Reducer de 530 104 à 6 934 (−98,69 %) sur `product_count`, sans changer le
résultat. Détails : [docs/hadoop_mapreduce.md](docs/hadoop_mapreduce.md).

## 9. Documentation

- [Modèle MongoDB et contrat d'interface](docs/mongodb_model.md)
- [Guide utilisateur du dashboard](docs/guide_utilisateur.md)
- [Hadoop et MapReduce](docs/hadoop_mapreduce.md)
- [Données et échantillons](data/README.md)

## 10. Limites

- L'inventaire est simulé : les transactions ne donnent ni stock initial ni réapprovisionnement.
- La couverture de stock suppose un rythme de vente constant (moyenne des 30 derniers jours).
- Les dates du dataset n'ont pas de fuseau : elles sont traitées comme UTC.
- `sales_daily` est agrégé avec pandas ; un job MapReduce de clé `(date, product_id)` pourrait le remplacer.
