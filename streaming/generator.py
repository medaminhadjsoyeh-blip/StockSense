#!/usr/bin/env python3
"""Generateur de ventes simulees pour le topic Kafka "sales"."""
import argparse
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

RESULTS = Path(__file__).resolve().parents[1] / "results"
FMT = "%Y-%m-%d %H:%M:%S"
ANOMALIES = ["quantity_high", "price_negative", "price_far", "bad_total",
             "duplicate_id", "invalid_json", "missing_field", "late"]


def load_catalog():
    prices = json.loads((RESULTS / "average_price.json").read_text(encoding="utf-8"))
    customers = json.loads((RESULTS / "customer_total.json").read_text(encoding="utf-8"))
    # Vrais produits seulement (pas DOT, M, POST...) et prix assez grands pour varier
    products = [p for p in prices
                if p["product_id"][:5].isdigit() and p["average_price"] >= 0.10]
    return products, [c["customer_id"] for c in customers]


class Generator:
    def __init__(self, products, customers):
        self.products = products
        self.customers = customers
        self.counter = 0
        self.last_id = None

    def normal(self):
        self.counter += 1
        product = random.choice(self.products)
        quantity = random.randint(1, 12)
        price = round(product["average_price"] * random.uniform(0.9, 1.1), 2)
        self.last_id = "S{}-{:05d}".format(int(time.time() * 1000), self.counter)
        return {
            "transaction_id": self.last_id,
            "timestamp": datetime.now().strftime(FMT),
            "customer_id": random.choice(self.customers),
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": price,
            "total_amount": round(quantity * price, 2),
        }

    def abnormal(self, kind):
        """Retourne une liste de messages (texte JSON ou texte casse)."""
        sale = self.normal()
        if kind == "quantity_high":
            sale["quantity"] = 800
            sale["total_amount"] = round(800 * sale["unit_price"], 2)
        elif kind == "price_negative":
            sale["unit_price"] = -abs(sale["unit_price"])
            sale["total_amount"] = round(sale["quantity"] * sale["unit_price"], 2)
        elif kind == "price_far":
            sale["unit_price"] = round(sale["unit_price"] * 5, 2)
            sale["total_amount"] = round(sale["quantity"] * sale["unit_price"], 2)
        elif kind == "bad_total":
            sale["total_amount"] = round(sale["total_amount"] + 9, 2)
        elif kind == "duplicate_id":
            return [json.dumps(sale), json.dumps(sale)]
        elif kind == "invalid_json":
            return ['{"transaction_id": "CASSE", "quantity":']
        elif kind == "missing_field":
            del sale["product_id"]
        elif kind == "late":
            old = datetime.now() - timedelta(minutes=20)
            sale["timestamp"] = old.strftime(FMT)
        return [json.dumps(sale)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100, help="nombre de ventes")
    parser.add_argument("--delay", type=float, default=0.5, help="secondes entre ventes")
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--inject", choices=ANOMALIES, help="envoie une seule anomalie")
    parser.add_argument("--server", default="localhost:9092")
    parser.add_argument("--dry-run", action="store_true", help="affiche sans envoyer")
    args = parser.parse_args()

    gen = Generator(*load_catalog())
    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=args.server)

    def publish(message):
        if args.dry_run:
            print(message)
        else:
            producer.send("sales", message.encode("utf-8"))

    if args.inject:
        for message in gen.abnormal(args.inject):
            publish(message)
        print("Anomalie envoyee :", args.inject)
    else:
        for _ in range(args.count):
            if random.random() < args.anomaly_rate:
                messages = gen.abnormal(random.choice(ANOMALIES))
            else:
                messages = [json.dumps(gen.normal())]
            for message in messages:
                publish(message)
            time.sleep(args.delay)
        print("Termine :", args.count, "ventes")

    if producer:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
