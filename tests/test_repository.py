"""Tests des fonctions d'accès MongoDB (base réelle jetable, ignorés sans serveur)."""

from datetime import datetime, timezone

import pytest

from database import repository, schema


def d(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def daily(day, pid, count, qty, revenue, price_sum):
    return {"date": d(day), "product_id": pid, "transaction_count": count,
            "quantity": qty, "revenue": revenue, "unit_price_sum": price_sum}


@pytest.fixture
def sales(db):
    repository.upsert_many(db, "sales_daily", [
        daily(1, "P1", 2, 3, 30.0, 20.0),
        daily(2, "P1", 1, 1, 10.0, 10.0),
        daily(2, "P2", 1, 5, 25.0, 5.0),
        daily(3, "POST", 1, 1, 18.0, 18.0),
    ])
    return db


def test_upsert_idempotent(db):
    docs = [{"product_id": "P1", "total_revenue": 10.0}, {"product_id": "P2", "total_revenue": 5.0}]
    first = repository.upsert_many(db, "product_revenue", docs)
    second = repository.upsert_many(db, "product_revenue", docs)
    assert first["upserted"] == 2
    assert second["upserted"] == 0
    assert db.product_revenue.count_documents({}) == 2


def test_upsert_met_a_jour_la_valeur(db):
    repository.upsert_many(db, "product_revenue", [{"product_id": "P1", "total_revenue": 1.0}])
    repository.upsert_many(db, "product_revenue", [{"product_id": "P1", "total_revenue": 2.0}])
    assert db.product_revenue.find_one({"product_id": "P1"})["total_revenue"] == 2.0


def test_upsert_refuse_cle_manquante(db):
    with pytest.raises(ValueError, match="clé manquante"):
        repository.upsert_many(db, "product_revenue", [{"total_revenue": 1.0}])


def test_collection_hors_contrat_refusee(db):
    with pytest.raises(ValueError, match="inconnue"):
        repository.upsert_many(db, "nope", [{"x": 1}])


def test_sync_supprime_les_documents_obsoletes(db):
    repository.sync_collection(db, "top10", [{"rank": r, "product_id": f"P{r}", "total_revenue": 1.0} for r in range(1, 11)])
    stats = repository.sync_collection(db, "top10", [{"rank": 1, "product_id": "P9", "total_revenue": 9.0}])
    assert stats["deleted"] == 9
    assert repository.get_top10(db) == [{"rank": 1, "product_id": "P9", "total_revenue": 9.0}]


def test_sync_avec_scope_ne_touche_pas_au_reste(db):
    stream_alert = {"alert_id": "QTY:T1", "type": "QUANTITY_ABOVE_THRESHOLD", "severity": "warning",
                    "reason": "q > 100", "detected_at": d(1), "source": "streaming"}
    repository.upsert_alerts(db, [stream_alert])
    batch = {"alert_id": "LOW_STOCK:P1", "type": "LOW_STOCK", "severity": "warning",
             "reason": "x", "detected_at": d(1), "source": "batch"}
    repository.sync_collection(db, "alerts", [batch], scope={"source": "batch", "type": "LOW_STOCK"})
    repository.sync_collection(db, "alerts", [], scope={"source": "batch", "type": "LOW_STOCK"})
    remaining = [a["alert_id"] for a in repository.get_alerts(db)]
    assert remaining == ["QTY:T1"]


def test_upsert_alerts_valide_le_contrat(db):
    base = {"alert_id": "A1", "severity": "warning", "reason": "r", "detected_at": d(1), "source": "streaming"}
    with pytest.raises(ValueError, match="Type"):
        repository.upsert_alerts(db, [{**base, "type": "UNKNOWN"}])
    with pytest.raises(ValueError, match="Sévérité"):
        repository.upsert_alerts(db, [{**base, "type": "LOW_STOCK", "severity": "high"}])
    repository.upsert_alerts(db, [{**base, "type": "LOW_STOCK"}, {**base, "type": "LOW_STOCK"}])
    assert db.alerts.count_documents({}) == 1
    assert db.alerts.find_one()["status"] == "open"


def test_totaux_sur_periode_borne_fin_exclusive(sales):
    totals = repository.sales_totals(sales, start=d(1), end=d(3))
    assert totals["revenue"] == pytest.approx(65.0)
    assert totals["transaction_count"] == 4
    assert totals["products"] == 2
    assert totals["days"] == 2


def test_totaux_periode_vide(sales):
    totals = repository.sales_totals(sales, start=d(20), end=d(21))
    assert totals["revenue"] == 0 and totals["products"] == 0


def test_filtre_produit_et_exclusion(sales):
    assert repository.sales_totals(sales, product_ids=["P2"])["revenue"] == pytest.approx(25.0)
    assert repository.sales_totals(sales, exclude_ids=["POST"])["revenue"] == pytest.approx(65.0)
    assert repository.sales_totals(sales)["revenue"] == pytest.approx(83.0)


def test_ventes_par_jour_triees(sales):
    rows = repository.sales_by_day(sales)
    assert [r["date"] for r in rows] == [d(1), d(2), d(3)]
    assert rows[1]["revenue"] == pytest.approx(35.0)


def test_top_produits_et_prix_moyen_pondere(sales):
    rows = repository.sales_by_product(sales, limit=2, exclude_ids=["POST"])
    assert [r["product_id"] for r in rows] == ["P1", "P2"]
    p1 = rows[0]
    assert p1["revenue"] == pytest.approx(40.0)
    # (20 + 10) / 3 lignes = 10, et non la moyenne des moyennes (10 + 10) / 2.
    assert p1["average_price"] == pytest.approx(10.0)
    by_qty = repository.sales_by_product(sales, sort_by="quantity", limit=1)
    assert by_qty[0]["product_id"] == "P2"


def test_bornes_de_dates(sales, db):
    assert repository.sales_date_bounds(sales) == (d(1), d(3))
    assert repository.sales_date_bounds(db.client["stocksense_vide_inexistante"]) is None


def test_inventaire_trie_par_couverture(db):
    now = d(1)
    items = [
        {"product_id": "A", "initial_stock": 10, "reorder_threshold": 1, "current_stock": 10,
         "coverage_days": None, "updated_at": now, "source": "batch"},
        {"product_id": "B", "initial_stock": 10, "reorder_threshold": 1, "current_stock": 10,
         "coverage_days": 1.5, "updated_at": now, "source": "batch"},
        {"product_id": "C", "initial_stock": 10, "reorder_threshold": 1, "current_stock": 10,
         "coverage_days": 8.0, "updated_at": now, "source": "batch"},
    ]
    repository.upsert_many(db, "inventory", items)
    assert [r["product_id"] for r in repository.get_inventory(db)] == ["B", "C", "A"]
    assert [r["product_id"] for r in repository.get_inventory(db, low_only=True)] == ["B"]


def test_liste_produits_repli_sur_mapreduce(db):
    repository.upsert_many(db, "product_revenue", [{"product_id": "Z", "total_revenue": 1.0}])
    assert repository.list_products(db) == [{"product_id": "Z", "description": ""}]


def test_validateurs_couvrent_tous_les_champs_requis():
    for spec in schema.ALL_COLLECTIONS:
        validator = spec.validator()["$jsonSchema"]
        assert set(spec.key) <= set(validator["required"]), spec.name
        assert set(validator["required"]) <= set(validator["properties"]), spec.name
