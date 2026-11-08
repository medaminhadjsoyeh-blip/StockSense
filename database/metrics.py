"""Calculs purs (sans MongoDB), testables isolément.

Ces fonctions sont la référence partagée entre le chargement batch,
le dashboard et les tests.
"""

from __future__ import annotations

import math

import pandas as pd

# Seuil d'alerte de couverture de stock (cahier des charges : 3 jours).
COVERAGE_ALERT_DAYS = 3.0
# Fenêtre (jours calendaires) utilisée pour la vente moyenne journalière.
AVG_SALES_WINDOW_DAYS = 30

TRANSACTION_COLUMNS = (
    "transaction_id",
    "timestamp",
    "customer_id",
    "product_id",
    "description",
    "quantity",
    "unit_price",
    "total_amount",
    "country",
)


def stock_coverage_days(current_stock: float, avg_daily_sales: float) -> float | None:
    """jours_de_couverture = stock_disponible / ventes_moyennes_journalieres.

    Retourne None si le produit ne se vend pas (couverture infinie / non définie).
    """
    if avg_daily_sales is None or avg_daily_sales <= 0 or math.isnan(avg_daily_sales):
        return None
    return max(float(current_stock), 0.0) / float(avg_daily_sales)


def is_low_stock(
    current_stock: float,
    reorder_threshold: float,
    coverage_days: float | None,
    alert_days: float = COVERAGE_ALERT_DAYS,
) -> bool:
    """Stock bas si sous le seuil de réapprovisionnement ou couverture < alert_days."""
    if current_stock <= reorder_threshold:
        return True
    return coverage_days is not None and coverage_days < alert_days


def read_transactions(path, chunksize: int | None = None):
    """Lit le CSV nettoyé (schéma de scripts/clean_data.py) avec des types stables."""
    return pd.read_csv(
        path,
        dtype={"transaction_id": str, "customer_id": str, "product_id": str, "description": str},
        keep_default_na=False,
        na_values={"quantity": [""], "unit_price": [""], "total_amount": [""]},
        parse_dates=["timestamp"],
        chunksize=chunksize,
    )


def aggregate_daily_sales(transactions: pd.DataFrame) -> pd.DataFrame:
    """Agrège les lignes de vente par (jour, produit).

    transaction_count compte les lignes de vente, comme le job MapReduce
    product_count. unit_price_sum permet de recalculer sur n'importe quelle
    période la moyenne arithmétique des prix (= job average_price) sans
    moyenner des moyennes.
    """
    if transactions.empty:
        return pd.DataFrame(
            columns=["date", "product_id", "transaction_count", "quantity", "revenue", "unit_price_sum"]
        )
    frame = transactions.assign(date=pd.to_datetime(transactions["timestamp"]).dt.normalize())
    daily = (
        frame.groupby(["date", "product_id"], sort=True)
        .agg(
            transaction_count=("product_id", "size"),
            quantity=("quantity", "sum"),
            revenue=("total_amount", "sum"),
            unit_price_sum=("unit_price", "sum"),
        )
        .reset_index()
    )
    return daily


def combine_daily(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Fusionne des agrégats partiels (lecture par blocs) : additions uniquement."""
    if not parts:
        return aggregate_daily_sales(pd.DataFrame())
    merged = pd.concat(parts, ignore_index=True)
    return (
        merged.groupby(["date", "product_id"], sort=True)[
            ["transaction_count", "quantity", "revenue", "unit_price_sum"]
        ]
        .sum()
        .reset_index()
    )


def product_descriptions(transactions: pd.DataFrame) -> pd.DataFrame:
    """Description la plus fréquente par produit (ordre alphabétique en cas d'égalité)."""
    counts = (
        transactions.groupby(["product_id", "description"]).size().reset_index(name="n")
    )
    return combine_descriptions([counts])


def combine_descriptions(parts: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(parts, ignore_index=True)
    merged = merged.groupby(["product_id", "description"], as_index=False)["n"].sum()
    merged = merged.sort_values(["product_id", "n", "description"], ascending=[True, False, True])
    return merged.drop_duplicates("product_id")[["product_id", "description"]].reset_index(drop=True)


def average_daily_sales(daily: pd.DataFrame, window_days: int = AVG_SALES_WINDOW_DAYS) -> pd.Series:
    """Quantité moyenne vendue par jour calendaire sur les window_days derniers jours.

    Les jours sans vente comptent pour zéro : on divise par la taille de la
    fenêtre, pas par le nombre de jours actifs.
    """
    if daily.empty:
        return pd.Series(dtype=float, name="avg_daily_sales")
    end = pd.to_datetime(daily["date"]).max()
    start = end - pd.Timedelta(days=window_days - 1)
    recent = daily[pd.to_datetime(daily["date"]) >= start]
    return (recent.groupby("product_id")["quantity"].sum() / window_days).rename("avg_daily_sales")


def resample_period(series_frame: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Regroupe une série journalière (colonne date) par jour, semaine ou mois.

    Les sommes sont additives ; average_price est recalculé à partir des
    composantes (unit_price_sum / transaction_count).
    """
    rule = {"day": "D", "week": "W-MON", "month": "MS"}[freq]
    if series_frame.empty:
        return series_frame
    frame = series_frame.set_index(pd.to_datetime(series_frame["date"])).drop(columns="date")
    if freq == "week":
        out = frame.resample(rule, label="left", closed="left").sum()
    else:
        out = frame.resample(rule).sum()
    out = out.reset_index().rename(columns={"index": "date"})
    if "date" not in out.columns:
        out = out.rename(columns={out.columns[0]: "date"})
    return out
