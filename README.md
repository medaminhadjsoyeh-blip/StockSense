# StockSense – Big Data Retail Analytics

## 1. Présentation

StockSense est un projet Big Data consacré à l'analyse des ventes
dans le secteur du commerce de détail.

L'objectif est de nettoyer, traiter et analyser les transactions
avec Hadoop MapReduce, puis de stocker les résultats dans MongoDB.

## 2. Technologies utilisées

- Python 3
- Pandas
- Hadoop 3.2.1
- Hadoop Streaming
- HDFS
- Docker
- MongoDB 7.0

## 3. Architecture du projet

Structure principale :
- data/raw/ : dataset original.
- data/processed/ : dataset nettoyé.
- scripts/ : scripts Python.
- mapreduce/src/ : Mapper, Combiner et Reducer.
- results/ : résultats TSV et JSON.
- docs/ : documentation technique.

## 4. Dataset

Dataset utilisé : UCI Online Retail.

- Lignes initiales : 541909
- Lignes après nettoyage : 530104
- Produits distincts : 3922
- Clients identifiés : 4338
- Chiffre d'affaires total : 10666684.54

Le nettoyage est réalisé par scripts/clean_data.py.

## 5. Traitements MapReduce

Cinq jobs ont été développés :

| Job | Description |
|---|---|
| product_count | Nombre de ventes et quantité par produit |
| product_revenue | Chiffre d'affaires par produit |
| customer_total | Dépenses totales par client |
| average_price | Prix unitaire moyen par produit |
| top10 | Classement des dix produits les plus rentables |

## 6. Optimisation avec Combiner

Benchmark du job product_count :

| Mesure | Sans Combiner | Avec Combiner |
|---|---:|---:|
| Map output records | 530104 | 530104 |
| Reduce input records | 530104 | 6934 |

Réduction du volume transmis au Reducer : 98.69 %.

Les résultats finaux sont identiques avec et sans Combiner.

## 7. Stockage MongoDB

Base de données : stocksense.

| Collection | Documents |
|---|---:|
| product_count | 3922 |
| product_revenue | 3922 |
| customer_total | 4338 |
| average_price | 3922 |
| top10 | 10 |
| data_quality_report | 1 |
| Total | 16115 |

Les résultats sont exportés en TSV et JSON.

## 8. Documentation

Documentation technique détaillée : docs/hadoop_mapreduce.md

## 9. Partie réalisée

Étudiant 1 : Data Engineering, nettoyage des données,
HDFS, Hadoop MapReduce, Combiner, exportation JSON et MongoDB.

