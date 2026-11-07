"""Couche de données du dashboard : appels repository + cache Streamlit.

Toutes les fonctions prennent des arguments hachables (tuples, dates) pour
que st.cache_data puisse mémoriser les résultats. Le TTL court permet de
voir les écritures du streaming sans recharger la page.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pandas as pd
import streamlit as st

from database import repository, verify
from database.client import get_client, get_database, ping
from database.config import get_settings

TTL = 60


@st.cache_resource(show_spinner=False)
def _client():
    return get_client(get_settings())


def db():
    return get_database(_client(), get_settings())


def is_available() -> bool:
    return ping(_client())


def to_utc(day: date | None, end_of_day: bool = False) -> datetime | None:
    """Date du filtre -> datetime UTC. sales_daily stocke minuit UTC."""
    if day is None:
        return None
    moment = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return moment + timedelta(days=1) if end_of_day else moment


def _ids(product_ids) -> list[str] | None:
    return list(product_ids) if product_ids else None


@st.cache_data(ttl=TTL, show_spinner=False)
def counts() -> dict[str, int]:
    return repository.collection_counts(db())


@st.cache_data(ttl=TTL, show_spinner=False)
def runs() -> list[dict]:
    return repository.get_pipeline_runs(db())


@st.cache_data(ttl=TTL, show_spinner=False)
def quality_report() -> dict | None:
    return repository.get_quality_report(db())


@st.cache_data(ttl=TTL, show_spinner=False)
def products() -> pd.DataFrame:
    return pd.DataFrame(repository.list_products(db()), columns=["product_id", "description"])


@st.cache_data(ttl=TTL, show_spinner=False)
def date_bounds() -> tuple[date, date] | None:
    bounds = repository.sales_date_bounds(db())
    if not bounds:
        return None
    return bounds[0].date(), bounds[1].date()


@st.cache_data(ttl=TTL, show_spinner=False)
def totals(start: date, end: date, product_ids: tuple, exclude: tuple) -> dict:
    """Totaux sur [start, end] (bornes incluses, jours entiers)."""
    return repository.sales_totals(
        db(), to_utc(start), to_utc(end, end_of_day=True), _ids(product_ids), _ids(exclude)
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def daily_series(start: date, end: date, product_ids: tuple, exclude: tuple) -> pd.DataFrame:
    rows = repository.sales_by_day(
        db(), to_utc(start), to_utc(end, end_of_day=True), _ids(product_ids), _ids(exclude)
    )
    frame = pd.DataFrame(rows, columns=["date", "transaction_count", "quantity", "revenue", "unit_price_sum"])
    if not frame.empty:
        frame["date"] = pd.to_datetime(frame["date"]).dt.tz_localize(None)
    return frame


@st.cache_data(ttl=TTL, show_spinner=False)
def by_product(start: date, end: date, product_ids: tuple, exclude: tuple, sort_by: str, limit: int | None) -> pd.DataFrame:
    rows = repository.sales_by_product(
        db(), to_utc(start), to_utc(end, end_of_day=True), _ids(product_ids),
        sort_by=sort_by, limit=limit, exclude_ids=_ids(exclude),
    )
    return pd.DataFrame(rows, columns=["product_id", "transaction_count", "quantity", "revenue", "unit_price_sum", "average_price"])


@st.cache_data(ttl=TTL, show_spinner=False)
def top10_reference() -> pd.DataFrame:
    return pd.DataFrame(repository.get_top10(db()), columns=["rank", "product_id", "total_revenue"])


@st.cache_data(ttl=TTL, show_spinner=False)
def product_stats() -> pd.DataFrame:
    return pd.DataFrame(
        repository.get_product_stats(db()),
        columns=["product_id", "description", "transaction_count", "total_quantity", "total_revenue", "average_price"],
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def customers() -> pd.DataFrame:
    return pd.DataFrame(repository.get_customers(db()), columns=["customer_id", "total_spent"])


@st.cache_data(ttl=TTL, show_spinner=False)
def inventory(product_ids: tuple) -> pd.DataFrame:
    return pd.DataFrame(
        repository.get_inventory(db(), _ids(product_ids)),
        columns=["product_id", "current_stock", "reorder_threshold", "avg_daily_sales", "coverage_days",
                 "initial_stock", "source", "updated_at"],
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def alerts(since: datetime | None, product_ids: tuple, types: tuple) -> pd.DataFrame:
    rows = repository.get_alerts(db(), start=since, product_ids=_ids(product_ids), types=_ids(types))
    return pd.DataFrame(
        rows,
        columns=["detected_at", "type", "severity", "product_id", "transaction_id", "reason", "value",
                 "threshold", "source", "status", "alert_id"],
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def sales_windows(since: datetime | None) -> pd.DataFrame:
    return pd.DataFrame(
        repository.get_sales_windows(db(), since=since),
        columns=["window_start", "window_end", "transaction_count", "quantity", "revenue"],
    )


@st.cache_data(ttl=300, show_spinner="Contrôles de cohérence…")
def consistency_checks() -> pd.DataFrame:
    return pd.DataFrame(verify.run_checks(db()))


def clear_cache() -> None:
    st.cache_data.clear()
