# StockSense – Étudiant 1
## Data Engineering, Hadoop, MapReduce et MongoDB

## 1. Objectif
Construire une chaîne Big Data batch pour analyser les ventes :
- Nettoyage et validation du dataset.
- Stockage des données dans HDFS.
- Traitements distribués avec Hadoop MapReduce.
- Optimisation avec Combiner.
- Exportation des résultats en JSON.
- Stockage dans MongoDB.

## 2. Environnement
- Windows 11 et WSL2 Ubuntu 24.04
- Docker
- Hadoop 3.2.1 et Hadoop Streaming
- Python 3
- MongoDB 7.0

## 3. Dataset et nettoyage

Dataset utilisé : UCI Online Retail.

- Fichier initial : data/raw/online_retail.xlsx
- Fichier nettoyé : data/processed/retail_clean.csv
- Script : scripts/clean_data.py

Le dataset initial contient 541909 lignes et 8 colonnes.

Le nettoyage comprend :
- Renommage et conversion des colonnes.
- Exclusion des factures annulées.
- Suppression des quantités non positives.
- Suppression des prix non positifs.
- Remplacement des descriptions manquantes par UNKNOWN.
- Calcul du montant total : quantity * unit_price.

### Résultats du nettoyage

| Indicateur | Valeur |
|---|---:|
| Lignes initiales | 541909 |
| Lignes finales | 530104 |
| Lignes supprimées | 11805 |
| Factures annulées détectées | 9288 |
| Quantités non positives détectées | 10624 |
| Prix non positifs détectés | 2517 |
| Clients manquants après nettoyage | 132220 |
| Descriptions manquantes après nettoyage | 0 |
| Produits distincts | 3922 |
| Clients distincts | 4338 |
| Chiffre d'affaires | 10666684.54 |

Les catégories de lignes supprimées peuvent se chevaucher.

## 4. Stockage HDFS

Les fichiers ont été chargés dans HDFS :

- /stocksense/input/retail_clean.csv
- /stocksense/input/data_quality_report.json

Les résultats MapReduce sont enregistrés dans /stocksense/output/.

Le fichier CSV contient 530104 lignes de données, plus une ligne d'en-tête.

## 5. Architecture MapReduce

Les scripts utilisés sont :
- mapreduce/src/mapper.py
- mapreduce/src/combiner.py
- mapreduce/src/reducer.py

Fonctionnement :
1. Mapper : lit les lignes et émet les clés et valeurs.
2. Combiner : effectue une agrégation locale.
3. Shuffle and Sort : regroupe les valeurs par clé.
4. Reducer : calcule les résultats définitifs.

## 6. Les cinq jobs MapReduce

### 6.1 Product count
Calcule le nombre de lignes de vente et la quantité totale par produit.
Résultat : 3922 produits.

### 6.2 Product revenue
Calcule le chiffre d'affaires total par produit.
Résultat : 3922 produits.

### 6.3 Customer total
Calcule le montant total dépensé par client.
Les identifiants clients manquants sont exclus.
Résultat : 4338 clients.

### 6.4 Average price
Calcule la moyenne arithmétique des prix unitaires par produit.
Le calcul utilise la somme des prix et le nombre de valeurs.
Résultat : 3922 produits.

### 6.5 Top 10
Classe les produits par chiffre d'affaires décroissant.
Un seul Reducer garantit le classement global.
Résultat : 10 produits.

### Classement des dix premiers produits

| Rang | Produit | Chiffre d'affaires |
|---:|---|---:|
| 1 | DOT | 206248.77 |
| 2 | 22423 | 174484.74 |
| 3 | 23843 | 168469.60 |
| 4 | 85123A | 104518.80 |
| 5 | 47566 | 99504.33 |
| 6 | 85099B | 94340.05 |
| 7 | 23166 | 81700.92 |
| 8 | M | 78110.27 |
| 9 | POST | 78101.88 |
| 10 | 23084 | 66964.99 |

## 7. Benchmark du Combiner

Le benchmark a été réalisé sur le job product_count.

| Mesure | Sans Combiner | Avec Combiner |
|---|---:|---:|
| Map output records | 530104 | 530104 |
| Combine output records | 0 | 6934 |
| Reduce input records | 530104 | 6934 |

Réduction du volume transmis au Reducer :

(530104 - 6934) / 530104 * 100 = 98.69 %

Les résultats finaux ont la même empreinte MD5 :

e1976c290d29078bc15c9d533939395b

Le Combiner réduit donc fortement le volume intermédiaire sans modifier le résultat final.

## 8. Exportation des résultats

Les résultats MapReduce ont été exportés au format TSV dans results/ :

- product_count.tsv
- product_revenue.tsv
- customer_total.tsv
- average_price.tsv
- top10.tsv

Le script scripts/export_to_json.py convertit ces résultats en fichiers JSON correspondants.

Fichiers JSON :
- product_count.json
- product_revenue.json
- customer_total.json
- average_price.json
- top10.json
- data_quality_report.json

## 9. Stockage MongoDB

MongoDB 7.0 fonctionne dans le conteneur Docker stocksense-mongodb.

Base de données : stocksense

L'importation est réalisée par le script scripts/import_mongodb.py.

| Collection | Nombre de documents |
|---|---:|
| product_count | 3922 |
| product_revenue | 3922 |
| customer_total | 4338 |
| average_price | 3922 |
| top10 | 10 |
| data_quality_report | 1 |
| **Total** | **16115** |

Les collections ont été vérifiées avec mongosh et countDocuments().

## 10. Conclusion

La partie Data Engineering de StockSense a permis de construire une chaîne
de traitement Big Data complète, depuis le nettoyage des données jusqu'au
stockage des résultats agrégés dans MongoDB.

Les cinq jobs MapReduce ont été exécutés avec succès.
Le benchmark montre une réduction de 98.69 % des enregistrements transmis
au Reducer grâce au Combiner.

Les résultats sont disponibles dans HDFS, exportés au format JSON
et importés dans six collections MongoDB.

