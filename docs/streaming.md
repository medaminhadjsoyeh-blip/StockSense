# StockSense - Étudiant 2 : Streaming Kafka / Spark

## 1. Objectif
Traiter des ventes simulées en continu, calculer des indicateurs par fenêtre,
détecter des anomalies par règles explicables et estimer la couverture de stock.

    Générateur Python -> topic Kafka "sales" -> Spark Structured Streaming -> MongoDB -> Dashboard

Cette partie illustre le streaming ; elle ne reproduit pas une infrastructure industrielle.

## 2. Environnement
| Outil | Version |
|---|---|
| Windows + WSL2 | Ubuntu 24.04 |
| Python | 3.12 |
| Java | OpenJDK 17 |
| Spark (PySpark) | 3.5.1, mode local |
| Kafka | 3.9.0 (image apache/kafka) |
| MongoDB | 7.0 (image mongo:7.0) |
| kafka-python / pymongo | 3.0.13 / 4.19.0 |

## 3. Fichiers
| Fichier | Rôle |
|---|---|
| streaming/generator.py | Producteur Kafka : ventes normales et anormales |
| streaming/spark_job.py | Lecture, contrôles, agrégations |
| streaming/mongo_sink.py | Écriture MongoDB idempotente, calcul du stock |
| streaming/build_inventory.py | Création de l'inventaire synthétique |
| streaming/check_mongo.py | Résumé du contenu MongoDB |
| data/inventory.csv | Inventaire synthétique de 3900 produits |
| tests/test_streaming.py | Tests unitaires |

## 4. Format d'un événement (topic `sales`)
```json
{"transaction_id": "S1794140873738-00001", "timestamp": "2026-11-08 13:27:00",
 "customer_id": "12347", "product_id": "10002",
 "quantity": 2, "unit_price": 1.09, "total_amount": 2.18}
```
| Champ | Type | Obligatoire |
|---|---|---|
| transaction_id | chaîne, unique | oui |
| timestamp | date `YYYY-MM-DD HH:MM:SS` | oui |
| customer_id | chaîne | non |
| product_id | chaîne | oui |
| quantity | entier | oui |
| unit_price | décimal | oui |
| total_amount | décimal | oui |

## 5. Lancement
Depuis la racine du dépôt, dans l'environnement virtuel :
```bash
docker start kafka stocksense-mongodb
python streaming/build_inventory.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming/spark_job.py
```
Dans un second terminal :
```bash
python streaming/generator.py --count 100 --delay 0.5
python streaming/generator.py --inject quantity_high
python streaming/check_mongo.py
```
Au premier lancement, créer les conteneurs et le topic :
```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
docker run -d --name stocksense-mongodb -p 27017:27017 mongo:7.0
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic sales --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

## 6. Règles d'anomalies
| Règle | Condition | Seuil |
|---|---|---|
| message_invalide | JSON illisible ou champ obligatoire absent | - |
| quantite_elevee | quantité > seuil | 500 |
| prix_negatif | prix <= 0 | 0 |
| prix_eloigne | écart au prix moyen de référence > seuil | 50 % |
| montant_incoherent | écart entre quantité x prix et montant > seuil | 0,01 |
| id_duplique | même identifiant répété dans une fenêtre de 10 minutes | 2 occurrences |
| stock_faible | couverture < seuil | 3 jours |

Le prix de référence vient de `results/average_price.json` (partie batch).
Le seuil de 500 est un seuil de démonstration, très supérieur aux quantités
normales simulées (1 à 12).

## 7. Indicateurs et stock
Les ventes valides, à prix et quantité positifs, sont dédupliquées par
`transaction_id`, puis agrégées par minute, au global et par produit.
Le retard toléré (watermark) est de 10 minutes.

    stock_disponible = stock_initial - quantité_vendue_simulée
    jours_de_couverture = stock_disponible / ventes_moyennes_journalières

La moyenne journalière vient de l'historique (quantité totale / 374 jours), car
quelques minutes de flux simulé ne donneraient pas un rythme représentatif.
Exemple : stock 12 et 6 ventes par jour donnent 2 jours, donc une alerte.

## 8. Collections MongoDB (base `stocksense`)
| Collection | `_id` stable | Principaux champs |
|---|---|---|
| sales_windows | début de fenêtre | window_start, window_end, sales_count, revenue, quantity |
| product_windows | produit + début de fenêtre | product_id, window_start, window_end, sales_count, revenue, quantity |
| stock_status | produit | initial_stock, quantity_sold, available_stock, avg_daily_sales, coverage_days, reorder_threshold, reorder_needed, low_coverage |
| alerts | règle + transaction ou produit | rule, motif, transaction_id, product_id et valeurs concernées |

L'écriture utilise un upsert avec `_id` stable : retraiter un lot écrase les mêmes
documents et n'ajoute pas de doublon.

## 9. Tests réalisés
| Test | Entrée | Résultat observé |
|---|---|---|
| Publication / consommation Kafka | 1 message de test | message relu à l'identique |
| Flux vide | aucun nouveau message | lots vides, pas d'arrêt du job |
| Flux normal | 10 ventes + 1 vente de quantité 800 | 11 ventes, CA 2759,37, quantité 856 (calcul manuel identique) |
| Message invalide | JSON cassé, champ absent | alerte message_invalide, le job continue |
| Règles d'anomalies | une anomalie de chaque type | une alerte avec le bon motif |
| Doublon | même identifiant envoyé deux fois | occurrences = 2, vente comptée une fois |
| Événement tardif | vente datée de -20 minutes | ignorée, aucune fenêtre créée |
| Bout en bout MongoDB | 20 ventes + 3 anomalies | 23 ventes comptées, 1 alerte par règle injectée |
| Volume accru | 1000 ventes en 11,36 s | 1023 ventes comptées au total, pas d'erreur |
| Tests unitaires | tests/test_streaming.py | 14 tests réussis |

Le test unitaire ne couvre pas Spark ; la chaîne Spark a été validée par les
exécutions manuelles ci-dessus. Le débit d'envoi mesuré est d'environ 88 ventes
par seconde. La latence de bout en bout n'a pas été mesurée.

## 10. Limites
- Inventaire synthétique, sans réapprovisionnement simulé.
- La couverture est une estimation : elle dépend du rythme historique. Les quantités
  simulées, élevées pour des petits stocks, font monter vite le nombre d'alertes.
- L'historique batch retire les factures annulées sans les déduire des achats
  d'origine : certains prix et moyennes peuvent en être affectés.
- Un événement en retard de plus de 10 minutes est ignoré, sans alerte.
- Les doublons ne sont détectés que dans une fenêtre de 10 minutes.
- Les règles sont simples : une vente au-dessus du seuil reste comptée dans le stock.
- Exécution locale sur une seule partition, écriture MongoDB depuis le driver Spark.
