"""StockSense — dashboard Streamlit.

Lancement : streamlit run dashboard/app.py
Variables : MONGO_URI (défaut mongodb://localhost:27017/), MONGO_DB (défaut stocksense).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import altair as alt  # noqa: E402
import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from dashboard import data  # noqa: E402
from dashboard.formatting import NON_PRODUCT_CODES, money, number, percent_delta  # noqa: E402
from database.config import get_settings  # noqa: E402
from database.metrics import COVERAGE_ALERT_DAYS, resample_period  # noqa: E402
from database.schema import ALERT_TYPES  # noqa: E402

st.set_page_config(page_title="StockSense", page_icon="📦", layout="wide")

GRANULARITY = {"Jour": "day", "Semaine": "week", "Mois": "month"}
LOAD_HINT = "python -m database.load_batch --transactions data/processed/retail_clean.csv --inventory data/inventory.csv"


def empty_state(message: str, command: str | None = None) -> None:
    st.info(message, icon="ℹ️")
    if command:
        st.code(command, language="bash")


# --- Connexion --------------------------------------------------------------------

settings = get_settings()
if not data.is_available():
    st.title("StockSense")
    st.error(f"MongoDB injoignable à l'adresse `{settings.mongo_uri}`.")
    st.markdown("Démarrer la base puis charger les données :")
    st.code("docker compose up -d\npython -m database.setup_db\n" + LOAD_HINT, language="bash")
    st.stop()

counts = data.counts()
product_ref = data.products()
labels = {
    row.product_id: f"{row.product_id} — {row.description}" if row.description else row.product_id
    for row in product_ref.itertuples()
}

# --- Barre latérale : filtres ---------------------------------------------------

with st.sidebar:
    st.title("📦 StockSense")
    st.caption(f"Base `{settings.mongo_db}`")

    bounds = data.date_bounds()
    if bounds:
        period = st.date_input(
            "Période", value=bounds, min_value=bounds[0], max_value=bounds[1], format="DD/MM/YYYY"
        )
        # Pendant la sélection, date_input renvoie une seule date.
        start, end = (period[0], period[1]) if len(period) == 2 else (period[0], period[0])
    else:
        start = end = None
        st.caption("Filtre de période indisponible : sales_daily est vide.")

    granularity = GRANULARITY[st.radio("Regroupement", list(GRANULARITY), horizontal=True)]
    selected = st.multiselect(
        "Produits",
        options=list(labels),
        format_func=lambda pid: labels.get(pid, pid),
        placeholder="Tous les produits",
    )
    exclude_fees = st.toggle(
        "Exclure frais et ajustements",
        value=True,
        help="Retire les codes qui ne sont pas des articles : " + ", ".join(NON_PRODUCT_CODES),
    )
    if st.button("Actualiser les données", width="stretch"):
        data.clear_cache()
        st.rerun()

    runs = data.runs()
    if runs:
        st.divider()
        for run in runs:
            when = run["finished_at"].astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
            st.caption(f"Dernier `{run['job']}` : {when}")

product_ids = tuple(selected)
exclude = tuple(NON_PRODUCT_CODES) if exclude_fees and not selected else ()
has_sales = bounds is not None

st.title("Analyse des ventes et du stock")
if has_sales:
    scope = f"{len(selected)} produit(s) sélectionné(s)" if selected else "tous les produits"
    st.caption(f"Du {start:%d/%m/%Y} au {end:%d/%m/%Y} · {scope}" + (" · hors frais" if exclude else ""))

tab_overview, tab_products, tab_customers, tab_stock, tab_quality = st.tabs(
    ["Vue d'ensemble", "Produits", "Clients", "Stock & alertes", "Qualité & cohérence"]
)

# --- Vue d'ensemble -----------------------------------------------------------------

with tab_overview:
    if has_sales:
        current = data.totals(start, end, product_ids, exclude)
        length = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=length - 1)
        previous = data.totals(prev_start, prev_end, product_ids, exclude)
        has_previous = prev_end >= bounds[0]

        def delta(key: str):
            return percent_delta(current[key], previous[key]) if has_previous else None

        avg_line = current["revenue"] / current["transaction_count"] if current["transaction_count"] else None
        cols = st.columns(5)
        cols[0].metric("Chiffre d'affaires", money(current["revenue"]), delta("revenue"))
        cols[1].metric("Quantité vendue", number(current["quantity"]), delta("quantity"))
        cols[2].metric("Lignes de vente", number(current["transaction_count"]), delta("transaction_count"))
        cols[3].metric("Produits vendus", number(current["products"]))
        cols[4].metric("CA moyen par ligne", money(avg_line))
        if has_previous:
            st.caption(f"Variations par rapport aux {length} jours précédents ({prev_start:%d/%m/%Y} – {prev_end:%d/%m/%Y}).")

        series = data.daily_series(start, end, product_ids, exclude)
        if series.empty:
            empty_state("Aucune vente sur cette période pour ces filtres.")
        else:
            grouped = resample_period(series, granularity)
            metric = st.segmented_control(
                "Indicateur", ["Chiffre d'affaires", "Quantité", "Lignes de vente"], default="Chiffre d'affaires"
            ) or "Chiffre d'affaires"
            column = {"Chiffre d'affaires": "revenue", "Quantité": "quantity", "Lignes de vente": "transaction_count"}[metric]
            chart = (
                alt.Chart(grouped)
                .mark_bar(color="#4F46E5")
                .encode(
                    x=alt.X("date:T", title=None),
                    y=alt.Y(f"{column}:Q", title=metric),
                    tooltip=[alt.Tooltip("date:T", title="Période"), alt.Tooltip(f"{column}:Q", title=metric, format=",.2f")],
                )
                .properties(height=340)
            )
            st.altair_chart(chart, width="stretch")
    else:
        report = data.quality_report()
        empty_state(
            "Les ventes par période ne sont pas chargées : seuls les totaux MapReduce sont disponibles.",
            LOAD_HINT,
        )
        if report:
            cols = st.columns(3)
            cols[0].metric("Chiffre d'affaires (total)", money(report["total_revenue"]))
            cols[1].metric("Produits distincts", number(report.get("unique_products")))
            cols[2].metric("Clients identifiés", number(report.get("unique_customers")))

# --- Produits -------------------------------------------------------------------------

with tab_products:
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Top 10 sur la période")
        if has_sales:
            sort_label = st.radio("Classer par", ["Chiffre d'affaires", "Quantité"], horizontal=True, key="top_sort")
            sort_by = "revenue" if sort_label == "Chiffre d'affaires" else "quantity"
            top = data.by_product(start, end, product_ids, exclude, sort_by, 10)
            if top.empty:
                empty_state("Aucune vente pour ces filtres.")
            else:
                top["label"] = top["product_id"].map(lambda pid: labels.get(pid, pid))
                chart = (
                    alt.Chart(top)
                    .mark_bar(color="#4F46E5")
                    .encode(
                        x=alt.X(f"{sort_by}:Q", title=sort_label),
                        y=alt.Y("label:N", sort="-x", title=None),
                        tooltip=["product_id", alt.Tooltip("revenue:Q", format=",.2f", title="CA"),
                                 alt.Tooltip("quantity:Q", title="Quantité")],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, width="stretch")
        else:
            empty_state("Classement par période indisponible sans sales_daily.", LOAD_HINT)

    with right:
        st.subheader("Top 10 MapReduce")
        st.caption("Référence batch, période complète, frais inclus.")
        reference = data.top10_reference()
        if reference.empty:
            empty_state("Collection top10 vide.", "python -m database.load_batch")
        else:
            reference["description"] = reference["product_id"].map(lambda pid: labels.get(pid, pid).split(" — ", 1)[-1])
            st.dataframe(
                reference[["rank", "product_id", "description", "total_revenue"]],
                hide_index=True,
                width="stretch",
                column_config={
                    "rank": st.column_config.NumberColumn("Rang", width="small"),
                    "product_id": "Produit",
                    "description": "Description",
                    "total_revenue": st.column_config.NumberColumn("CA (£)", format="%.2f"),
                },
            )

    st.subheader("Détail par produit")
    if has_sales:
        detail = data.by_product(start, end, product_ids, exclude, "revenue", None)
        detail.insert(1, "description", detail["product_id"].map(lambda pid: labels.get(pid, pid).split(" — ", 1)[-1]))
        st.caption(
            f"{len(detail)} produits. Le prix moyen est la moyenne arithmétique des prix unitaires "
            "sur la période, recalculée à partir de la somme et du nombre de prix : aucune moyenne de moyennes."
        )
        st.dataframe(
            detail.drop(columns=["unit_price_sum"]),
            hide_index=True,
            width="stretch",
            column_config={
                "product_id": "Produit",
                "description": "Description",
                "transaction_count": st.column_config.NumberColumn("Lignes de vente"),
                "quantity": st.column_config.NumberColumn("Quantité"),
                "revenue": st.column_config.NumberColumn("CA (£)", format="%.2f"),
                "average_price": st.column_config.NumberColumn("Prix moyen (£)", format="%.4f"),
            },
        )
    else:
        stats = data.product_stats()
        if stats.empty:
            empty_state("Aucun résultat produit en base.", "python -m database.load_batch")
        else:
            if selected:
                stats = stats[stats["product_id"].isin(selected)]
            st.dataframe(stats.sort_values("total_revenue", ascending=False), hide_index=True, width="stretch")

# --- Clients ---------------------------------------------------------------------------

with tab_customers:
    st.caption("Résultat du job MapReduce customer_total : période complète, clients identifiés uniquement. "
               "Les filtres de période et de produit ne s'appliquent pas à cet onglet.")
    clients = data.customers()
    if clients.empty:
        empty_state("Collection customer_total vide.", "python -m database.load_batch")
    else:
        total_spent = clients["total_spent"].sum()
        top_n = st.slider("Nombre de clients affichés", 5, 50, 10, step=5)
        top_clients = clients.head(top_n)
        share = top_clients["total_spent"].sum() / total_spent if total_spent else 0
        cols = st.columns(4)
        cols[0].metric("Clients identifiés", number(len(clients)))
        cols[1].metric("CA clients identifiés", money(total_spent))
        cols[2].metric("Dépense moyenne", money(clients["total_spent"].mean()))
        cols[3].metric(f"Part des {top_n} premiers", f"{share * 100:.1f} %".replace(".", ","))

        left, right = st.columns([3, 2])
        with left:
            chart = (
                alt.Chart(top_clients)
                .mark_bar(color="#0EA5E9")
                .encode(
                    x=alt.X("total_spent:Q", title="Montant total (£)"),
                    y=alt.Y("customer_id:N", sort="-x", title="Client", axis=alt.Axis(labelOverlap=False)),
                    tooltip=["customer_id", alt.Tooltip("total_spent:Q", format=",.2f", title="Montant (£)")],
                )
                .properties(height=max(260, 26 * top_n), title=f"Top {top_n} clients")
            )
            st.altair_chart(chart, width="stretch")
        with right:
            positive = clients[clients["total_spent"] > 0]
            hist = (
                alt.Chart(positive)
                .transform_calculate(log_spent="log(datum.total_spent) / LN10")
                .mark_bar(color="#94A3B8")
                .encode(
                    x=alt.X("log_spent:Q", bin=alt.Bin(maxbins=30), title="log10(montant total £)"),
                    y=alt.Y("count():Q", title="Clients"),
                )
                .properties(height=300, title="Répartition des dépenses (échelle log)")
            )
            st.altair_chart(hist, width="stretch")

        st.dataframe(
            clients,
            hide_index=True,
            width="stretch",
            column_config={
                "customer_id": "Client",
                "total_spent": st.column_config.NumberColumn("Montant total (£)", format="%.2f"),
            },
        )

# --- Stock & alertes -------------------------------------------------------------------

with tab_stock:
    st.subheader("Alertes")
    st.caption("Les alertes sont horodatées en temps réel : le filtre de période historique ne s'y applique pas.")
    col_since, col_types = st.columns([1, 3])
    since_label = col_since.selectbox("Depuis", ["24 heures", "7 jours", "30 jours", "Toujours"], index=3)
    since = None if since_label == "Toujours" else datetime.now(timezone.utc) - timedelta(
        hours={"24 heures": 24, "7 jours": 168, "30 jours": 720}[since_label]
    )
    types = col_types.multiselect("Types", ALERT_TYPES, placeholder="Tous les types")
    alerts = data.alerts(since, product_ids, tuple(types))
    if alerts.empty:
        empty_state(
            "Aucune alerte pour ces filtres. Les alertes d'anomalie sont produites par le job Spark "
            "(Étudiant 2) ; les alertes de stock bas par le chargement de l'inventaire."
        )
    else:
        by_type = alerts.groupby("type").size()
        cols = st.columns(min(len(by_type), 5) or 1)
        for col, (kind, n) in zip(cols, by_type.items()):
            col.metric(kind.replace("_", " ").capitalize(), number(n))
        alerts[["product_id", "transaction_id"]] = alerts[["product_id", "transaction_id"]].fillna("")
        st.dataframe(
            alerts.drop(columns=["alert_id"]),
            hide_index=True,
            width="stretch",
            column_config={
                "detected_at": st.column_config.DatetimeColumn("Détectée le", format="DD/MM/YYYY HH:mm"),
                "type": "Type",
                "severity": "Sévérité",
                "product_id": "Produit",
                "transaction_id": "Transaction",
                "reason": st.column_config.TextColumn("Motif", width="large"),
                "value": "Valeur",
                "threshold": "Seuil",
                "source": "Source",
                "status": "Statut",
            },
        )

    st.subheader("Couverture de stock")
    stock = data.inventory(product_ids)
    if stock.empty:
        empty_state("Inventaire non chargé.", LOAD_HINT)
    else:
        low = stock[(stock["current_stock"] <= stock["reorder_threshold"])
                    | (stock["coverage_days"].notna() & (stock["coverage_days"] < COVERAGE_ALERT_DAYS))]
        cols = st.columns(3)
        cols[0].metric("Produits suivis", number(len(stock)))
        cols[1].metric(f"Sous {COVERAGE_ALERT_DAYS:g} jours ou seuil", number(len(low)))
        cols[2].metric("Couverture médiane", f"{number(stock['coverage_days'].median(), 1)} j")
        st.caption("Couverture = stock disponible / ventes moyennes journalières (30 derniers jours). "
                   "Estimation : dépend du rythme de vente et des réapprovisionnements.")
        only_low = st.toggle("Afficher seulement les produits à réapprovisionner", value=True)
        shown = low if only_low else stock
        shown = shown.assign(description=shown["product_id"].map(lambda pid: labels.get(pid, pid).split(" — ", 1)[-1]))
        st.dataframe(
            shown[["product_id", "description", "current_stock", "reorder_threshold", "avg_daily_sales",
                   "coverage_days", "source", "updated_at"]],
            hide_index=True,
            width="stretch",
            column_config={
                "product_id": "Produit",
                "description": "Description",
                "current_stock": st.column_config.NumberColumn("Stock"),
                "reorder_threshold": st.column_config.NumberColumn("Seuil"),
                "avg_daily_sales": st.column_config.NumberColumn("Vente moy./jour", format="%.2f"),
                "coverage_days": st.column_config.ProgressColumn(
                    "Couverture (j)", format="%.1f", min_value=0, max_value=30
                ),
                "source": "Source",
                "updated_at": st.column_config.DatetimeColumn("Mis à jour", format="DD/MM/YYYY HH:mm"),
            },
        )

    st.subheader("Flux temps réel")
    windows = data.sales_windows(None)
    if windows.empty:
        empty_state("Aucune fenêtre de streaming en base (collection sales_windows). "
                    "Lancer le producteur Kafka et le job Spark pour alimenter ce graphique.")
    else:
        windows["window_start"] = pd.to_datetime(windows["window_start"])
        line = (
            alt.Chart(windows)
            .mark_line(point=True, color="#16A34A")
            .encode(
                x=alt.X("window_start:T", title="Début de fenêtre"),
                y=alt.Y("revenue:Q", title="CA de la fenêtre (£)"),
                tooltip=["window_start:T", "transaction_count:Q", "quantity:Q", alt.Tooltip("revenue:Q", format=",.2f")],
            )
            .properties(height=280)
        )
        st.altair_chart(line, width="stretch")

# --- Qualité & cohérence -------------------------------------------------------------

with tab_quality:
    report = data.quality_report()
    st.subheader("Rapport de nettoyage")
    if not report:
        empty_state("Rapport qualité absent.", "python -m database.load_batch")
    else:
        cols = st.columns(4)
        cols[0].metric("Lignes initiales", number(report.get("initial_rows")))
        cols[1].metric("Lignes valides", number(report.get("final_rows")))
        cols[2].metric("Lignes rejetées", number(report.get("removed_rows")))
        kept = report["final_rows"] / report["initial_rows"] if report.get("initial_rows") else None
        cols[3].metric("Taux de conservation", f"{kept * 100:.2f} %".replace(".", ",") if kept else "—")
        with st.expander("Détail du rapport"):
            st.json(report)

    st.subheader("Contrôles de cohérence")
    st.caption("Les chiffres affichés sont recoupés entre les résultats MapReduce, le rapport qualité "
               "et les agrégats journaliers (calculés indépendamment).")
    checks = data.consistency_checks()
    if not checks.empty:
        icons = {"pass": "✅", "fail": "❌", "skip": "⏭️"}
        checks.insert(0, "état", checks.pop("status").map(icons))
        checks[["expected", "actual"]] = checks[["expected", "actual"]].astype(str)
        st.dataframe(checks, hide_index=True, width="stretch",
                     column_config={"name": "Contrôle", "expected": "Attendu", "actual": "Obtenu", "detail": "Détail"})

    st.subheader("Collections MongoDB")
    st.dataframe(
        pd.DataFrame({"collection": list(counts), "documents": list(counts.values())}),
        hide_index=True,
        width="stretch",
    )
