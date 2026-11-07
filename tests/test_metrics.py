"""Tests unitaires des calculs purs, dont les critères d'acceptation du cahier (§8)."""

import pandas as pd
import pytest

from dashboard.formatting import money, number, percent_delta
from database import metrics


def tx(rows):
    frame = pd.DataFrame(rows, columns=["timestamp", "product_id", "quantity", "unit_price"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    frame["total_amount"] = (frame["quantity"] * frame["unit_price"]).round(2)
    return frame


def totals_by_product(daily):
    return daily.groupby("product_id")[["transaction_count", "quantity", "revenue", "unit_price_sum"]].sum()


# --- Critères d'acceptation §8 ------------------------------------------------

def test_comptage_p1_apparait_3_fois():
    daily = metrics.aggregate_daily_sales(tx([
        ("2024-01-01 10:00", "P1", 1, 1.0),
        ("2024-01-01 11:00", "P1", 1, 1.0),
        ("2024-01-02 09:00", "P1", 1, 1.0),
    ]))
    assert totals_by_product(daily).loc["P1", "transaction_count"] == 3


def test_somme_p1_10_20_5_vaut_35():
    daily = metrics.aggregate_daily_sales(tx([
        ("2024-01-01 10:00", "P1", 1, 10.0),
        ("2024-01-02 10:00", "P1", 1, 20.0),
        ("2024-01-03 10:00", "P1", 1, 5.0),
    ]))
    assert totals_by_product(daily).loc["P1", "revenue"] == pytest.approx(35.0)


def test_moyenne_p2_10_20_vaut_15_sans_moyenne_de_moyennes():
    # Deux jours avec des volumes différents : la moyenne des moyennes
    # journalières (10 + 20) / 2 coïncide ici, on ajoute donc un troisième prix.
    daily = metrics.aggregate_daily_sales(tx([
        ("2024-01-01 10:00", "P2", 1, 10.0),
        ("2024-01-02 10:00", "P2", 1, 20.0),
    ]))
    row = totals_by_product(daily).loc["P2"]
    assert row["unit_price_sum"] / row["transaction_count"] == pytest.approx(15.0)

    daily = metrics.aggregate_daily_sales(tx([
        ("2024-01-01 10:00", "P2", 1, 10.0),
        ("2024-01-01 11:00", "P2", 1, 10.0),
        ("2024-01-02 10:00", "P2", 1, 40.0),
    ]))
    row = totals_by_product(daily).loc["P2"]
    # Bonne réponse : (10 + 10 + 40) / 3 = 20, et non (10 + 40) / 2 = 25.
    assert row["unit_price_sum"] / row["transaction_count"] == pytest.approx(20.0)


def test_stock_12_vente_6_par_jour_donne_2_jours():
    assert metrics.stock_coverage_days(12, 6) == pytest.approx(2.0)


# --- Couverture et stock bas ------------------------------------------------------

@pytest.mark.parametrize("avg", [0, -1, None, float("nan")])
def test_couverture_indefinie_sans_vente(avg):
    assert metrics.stock_coverage_days(10, avg) is None


def test_couverture_stock_negatif_ramene_a_zero():
    assert metrics.stock_coverage_days(-5, 2) == 0


@pytest.mark.parametrize(
    "stock,threshold,coverage,expected",
    [
        (10, 5, 2.9, True),    # couverture < 3 j
        (10, 5, 3.0, False),   # limite : pas d'alerte à 3 j pile
        (5, 5, 10.0, True),    # stock au seuil
        (100, 5, None, False),  # pas de vente : pas d'alerte de couverture
    ],
)
def test_is_low_stock(stock, threshold, coverage, expected):
    assert metrics.is_low_stock(stock, threshold, coverage) is expected


def test_vente_moyenne_compte_les_jours_sans_vente():
    daily = pd.DataFrame({
        "date": pd.to_datetime(["2024-01-01", "2024-01-30"]),
        "product_id": ["P1", "P1"],
        "quantity": [30, 30],
    })
    # 60 unités sur une fenêtre de 30 jours calendaires -> 2 / jour.
    assert metrics.average_daily_sales(daily, window_days=30)["P1"] == pytest.approx(2.0)


def test_vente_moyenne_ignore_ventes_hors_fenetre():
    daily = pd.DataFrame({
        "date": pd.to_datetime(["2023-12-01", "2024-01-30"]),
        "product_id": ["P1", "P1"],
        "quantity": [1000, 30],
    })
    assert metrics.average_daily_sales(daily, window_days=30)["P1"] == pytest.approx(1.0)


# --- Agrégation par blocs ------------------------------------------------------------

def test_agregation_par_blocs_identique_a_agregation_globale(sample_csv):
    whole = metrics.aggregate_daily_sales(metrics.read_transactions(sample_csv))
    parts = [metrics.aggregate_daily_sales(c) for c in metrics.read_transactions(sample_csv, chunksize=137)]
    combined = metrics.combine_daily(parts)
    pd.testing.assert_frame_equal(
        whole.reset_index(drop=True), combined.reset_index(drop=True), check_dtype=False
    )


def test_description_la_plus_frequente():
    frame = pd.DataFrame({
        "product_id": ["P1", "P1", "P1", "P2"],
        "description": ["MUG", "MUG", "MUG RED", "BAG"],
    })
    result = metrics.product_descriptions(frame).set_index("product_id")["description"]
    assert result.to_dict() == {"P1": "MUG", "P2": "BAG"}


def test_resample_semaine_et_mois_conserve_les_sommes():
    days = pd.date_range("2024-01-01", "2024-02-29", freq="D")
    frame = pd.DataFrame({"date": days, "revenue": 1.0, "quantity": 2, "transaction_count": 1})
    for freq in ("day", "week", "month"):
        out = metrics.resample_period(frame, freq)
        assert out["revenue"].sum() == pytest.approx(len(days))
        assert out["quantity"].sum() == 2 * len(days)
    months = metrics.resample_period(frame, "month")
    assert list(months["revenue"]) == [31.0, 29.0]
    weeks = metrics.resample_period(frame, "week")
    assert weeks["date"].iloc[0] == pd.Timestamp("2024-01-01")  # lundi


# --- Mise en forme -------------------------------------------------------------------------

def test_formatage_francais():
    assert money(10666684.54) == "10 666 684,54 £"
    assert number(1234) == "1 234"
    assert money(None) == "—"
    assert percent_delta(110, 100) == "+10,0 %"
    assert percent_delta(5, 0) is None
