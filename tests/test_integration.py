"""Tests d'intégration bout en bout sur un vrai MongoDB.

Chaîne testée avec le même jeu de données pour tous les modules :
CSV synthétique -> jobs MapReduce de l'Étudiant 1 (exécutés en local)
-> export JSON -> load_batch -> MongoDB -> contrôles de cohérence -> dashboard.

Ignorés automatiquement si MongoDB n'est pas joignable.
"""

import csv
from pathlib import Path

import pandas as pd
import pytest
from pymongo.errors import WriteError

import generate_inventory
import local_mapreduce
from dashboard.formatting import NON_PRODUCT_CODES, money, number
from database import load_batch, repository, verify

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def pipeline(tmp_path, sample_csv, mongo_db):
    local_mapreduce.main(["--transactions", str(sample_csv), "--root", str(tmp_path)])
    inventory = tmp_path / "inventory.csv"
    generate_inventory.main(["--transactions", str(sample_csv), "--out", str(inventory)])
    counts = load_batch.run(mongo_db, tmp_path / "results", sample_csv, inventory)
    return {"db": mongo_db, "counts": counts, "results": tmp_path / "results", "inventory": inventory}


def write_csv(path: Path, rows: list[dict]) -> Path:
    columns = ["transaction_id", "timestamp", "customer_id", "product_id", "description",
               "quantity", "unit_price", "total_amount", "country"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for i, row in enumerate(rows, 1):
            writer.writerow({"transaction_id": f"T{i}", "timestamp": "2024-01-01 10:00:00", "description": "X",
                             "country": "UK", "customer_id": "", **row,
                             "total_amount": round(row["quantity"] * row["unit_price"], 2)})
    return path


def test_mapreduce_total_client_c1_12_et_8_vaut_20(tmp_path):
    path = write_csv(tmp_path / "c1.csv", [
        {"customer_id": "C1", "product_id": "P1", "quantity": 1, "unit_price": 12.0},
        {"customer_id": "C1", "product_id": "P2", "quantity": 1, "unit_price": 8.0},
        {"customer_id": "", "product_id": "P2", "quantity": 1, "unit_price": 99.0},
    ])
    output = local_mapreduce.run_job("customer_total", path.read_text(encoding="utf-8"))
    assert [line.split("\t")[0] for line in output] == ["C1"]
    assert float(output[0].split("\t")[1]) == pytest.approx(20.0)


def test_chaine_complete_coherente(pipeline, sample_csv):
    checks = verify.run_checks(pipeline["db"], sample_csv)
    failed = [c for c in checks if c["status"] != "pass"]
    assert not failed, failed
    assert len(checks) == 13


def test_rechargement_idempotent(pipeline, sample_csv):
    db = pipeline["db"]
    before = repository.collection_counts(db)
    load_batch.run(db, pipeline["results"], sample_csv, pipeline["inventory"])
    assert repository.collection_counts(db) == before


def test_validateur_refuse_un_document_invalide(pipeline):
    with pytest.raises(WriteError):
        pipeline["db"].product_revenue.insert_one({"product_id": 42, "total_revenue": "beaucoup"})


def test_index_uniques_crees(pipeline):
    indexes = pipeline["db"].sales_daily.index_information()
    assert indexes["uniq_date_product_id"]["unique"] is True


def test_alertes_stock_bas_avec_motif(pipeline):
    db = pipeline["db"]
    alerts = repository.get_alerts(db, types=["LOW_STOCK"])
    low = repository.get_inventory(db, low_only=True)
    assert alerts, "le générateur d'inventaire doit produire au moins une alerte"
    assert {a["product_id"] for a in alerts} == {r["product_id"] for r in low}
    assert all(a["reason"] for a in alerts)


def test_alertes_streaming_conservees_au_rechargement(pipeline, sample_csv):
    db = pipeline["db"]
    repository.upsert_alerts(db, [{
        "alert_id": "QUANTITY_ABOVE_THRESHOLD:T99999", "type": "QUANTITY_ABOVE_THRESHOLD",
        "severity": "critical", "product_id": "P001", "transaction_id": "T99999",
        "reason": "quantité 500 > seuil 100", "value": 500, "threshold": 100,
        "detected_at": repository.utcnow(), "source": "streaming",
    }])
    load_batch.run(db, pipeline["results"], sample_csv, pipeline["inventory"])
    assert db.alerts.count_documents({"source": "streaming"}) == 1


def test_chargement_sans_transactions_ni_inventaire(tmp_path, sample_csv, mongo_db):
    local_mapreduce.main(["--transactions", str(sample_csv), "--root", str(tmp_path)])
    counts = load_batch.run(mongo_db, tmp_path / "results", None, None)
    assert "sales_daily" not in counts
    checks = {c["name"]: c["status"] for c in verify.run_checks(mongo_db)}
    assert checks["sales_daily cohérent avec MapReduce"] == "skip"
    assert "fail" not in checks.values()


# --- Dashboard ----------------------------------------------------------------------

def run_app(monkeypatch, db_name: str):
    from streamlit.testing.v1 import AppTest

    import dashboard.data as data

    monkeypatch.setenv("MONGO_DB", db_name)
    data.clear_cache()
    return AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60).run()


def test_dashboard_affiche_des_chiffres_justes(pipeline, sample_csv, monkeypatch):
    app = run_app(monkeypatch, pipeline["db"].name)
    assert not app.exception, app.exception

    # Calcul indépendant avec pandas, filtre par défaut : toute la période, hors frais.
    frame = pd.read_csv(sample_csv, dtype={"product_id": str})
    goods = frame[~frame["product_id"].isin(NON_PRODUCT_CODES)]
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Chiffre d'affaires"] == money(goods["total_amount"].sum())
    assert metrics["Quantité vendue"] == number(goods["quantity"].sum())
    assert metrics["Lignes de vente"] == number(len(goods))
    assert metrics["Produits vendus"] == number(goods["product_id"].nunique())

    customers = frame.loc[frame["customer_id"].notna(), "customer_id"].nunique()
    assert metrics["Clients identifiés"] == number(customers)
    assert len(app.tabs) == 5


def test_dashboard_filtre_produit(pipeline, sample_csv, monkeypatch):
    app = run_app(monkeypatch, pipeline["db"].name)
    app.sidebar.multiselect[0].set_value(["P002"]).run()
    assert not app.exception
    frame = pd.read_csv(sample_csv, dtype={"product_id": str})
    expected = frame.loc[frame["product_id"] == "P002", "total_amount"].sum()
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Chiffre d'affaires"] == money(expected)


def test_dashboard_mode_batch_seul(tmp_path, sample_csv, mongo_db, monkeypatch):
    """Résultats MapReduce sans sales_daily : totaux du rapport qualité affichés."""
    local_mapreduce.main(["--transactions", str(sample_csv), "--root", str(tmp_path)])
    load_batch.run(mongo_db, tmp_path / "results", None, None)
    app = run_app(monkeypatch, mongo_db.name)
    assert not app.exception, app.exception
    report = repository.get_quality_report(mongo_db)
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Chiffre d'affaires (total)"] == money(report["total_revenue"])


def test_dashboard_base_vide(mongo_db, monkeypatch):
    app = run_app(monkeypatch, mongo_db.name)
    assert not app.exception, app.exception
    infos = " ".join(i.value for i in app.info)
    assert "ne sont pas chargées" in infos
