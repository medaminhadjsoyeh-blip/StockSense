#!/usr/bin/env python3
"""Tests sans Spark ni Kafka : generateur, stock et identifiants d'alerte."""
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))

import generator  # noqa: E402
import mongo_sink  # noqa: E402

TEST_ITEM = {"initial_stock": 12, "reorder_threshold": 5, "avg_daily_sales": 6.0}


class StockTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(mongo_sink.INVENTORY, {"T": TEST_ITEM})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_cahier_12_divise_par_6_donne_2_jours(self):
        doc = mongo_sink.stock_document("T", 0)
        self.assertEqual(doc["coverage_days"], 2.0)
        self.assertTrue(doc["low_coverage"])

    def test_vente_reduit_le_stock(self):
        doc = mongo_sink.stock_document("T", 3)
        self.assertEqual(doc["available_stock"], 9)
        self.assertEqual(doc["coverage_days"], 1.5)

    def test_stock_jamais_negatif(self):
        self.assertEqual(mongo_sink.stock_document("T", 20)["available_stock"], 0)

    def test_seuil_de_reapprovisionnement(self):
        self.assertFalse(mongo_sink.stock_document("T", 6)["reorder_needed"])
        self.assertTrue(mongo_sink.stock_document("T", 7)["reorder_needed"])


class AlertIdTests(unittest.TestCase):
    def row(self, transaction_id):
        return {"rule": "quantite_elevee", "transaction_id": transaction_id,
                "raw": "{casse", "quantity": 800}

    def written(self, rows):
        with patch.object(mongo_sink, "upsert") as upsert:
            mongo_sink.write_alerts(rows)
        return upsert.call_args[0][1]

    def test_identifiant_stable_et_motif(self):
        first = self.written([self.row("S1")])[0]
        again = self.written([self.row("S1")])[0]
        self.assertEqual(first["_id"], "quantite_elevee|S1")
        self.assertEqual(first["_id"], again["_id"])
        self.assertTrue(first["motif"])
        self.assertNotIn("raw", first)

    def test_message_sans_identifiant(self):
        doc = self.written([self.row(None)])[0]
        self.assertTrue(doc["_id"].startswith("quantite_elevee|"))
        self.assertEqual(len(doc["_id"].split("|")[1]), 64)


class GeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = generator.Generator(*generator.load_catalog())

    def one(self, kind):
        return json.loads(self.gen.abnormal(kind)[0])

    def test_vente_normale_coherente_et_unique(self):
        sales = [self.gen.normal() for _ in range(500)]
        self.assertEqual(len({s["transaction_id"] for s in sales}), 500)
        for sale in sales:
            self.assertAlmostEqual(sale["quantity"] * sale["unit_price"],
                                   sale["total_amount"], delta=0.011)

    def test_quantite_elevee(self):
        self.assertEqual(self.one("quantity_high")["quantity"], 800)

    def test_prix_negatif(self):
        self.assertLess(self.one("price_negative")["unit_price"], 0)

    def test_montant_incoherent(self):
        sale = self.one("bad_total")
        self.assertGreater(abs(sale["quantity"] * sale["unit_price"]
                               - sale["total_amount"]), 8)

    def test_doublon(self):
        messages = self.gen.abnormal("duplicate_id")
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0], messages[1])

    def test_message_illisible(self):
        with self.assertRaises(json.JSONDecodeError):
            json.loads(self.gen.abnormal("invalid_json")[0])

    def test_champ_manquant(self):
        self.assertNotIn("product_id", self.one("missing_field"))

    def test_vente_tardive(self):
        sale_time = datetime.strptime(self.one("late")["timestamp"], generator.FMT)
        self.assertGreaterEqual((datetime.now() - sale_time).total_seconds(), 19 * 60)


if __name__ == "__main__":
    unittest.main()
