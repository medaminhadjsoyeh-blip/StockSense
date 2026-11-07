import pandas as pd
import json
from pathlib import Path

BASE = Path.home() / "StockSense"
INPUT = BASE / "data/raw/online_retail.xlsx"
OUTPUT = BASE / "data/processed/retail_clean.csv"
REPORT = BASE / "results/data_quality_report.json"

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
REPORT.parent.mkdir(parents=True, exist_ok=True)

# 1. Chargement
df = pd.read_excel(INPUT)
initial_rows = len(df)

# 2. Renommer les colonnes
df = df.rename(columns={
    "InvoiceNo": "transaction_id",
    "StockCode": "product_id",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "timestamp",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country"
})

# 3. Nettoyage des types
df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
df["product_id"] = df["product_id"].astype(str).str.strip()
df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df["customer_id"] = df["customer_id"].apply(
    lambda x: str(int(x)) if pd.notna(x) else ""
)

# 4. Rapport des anomalies avant filtrage
cancelled = df["transaction_id"].str.startswith("C", na=False).sum()
invalid_quantity = (df["quantity"] <= 0).sum()
invalid_price = (df["unit_price"] <= 0).sum()
missing_customer = (df["customer_id"] == "").sum()
missing_description = df["description"].isna().sum()

# 5. Supprimer les lignes invalides
df = df.dropna(subset=[
    "transaction_id", "product_id", "quantity",
    "unit_price", "timestamp"
])

df = df[
    (~df["transaction_id"].str.startswith("C", na=False))
    & (df["quantity"] > 0)
    & (df["unit_price"] > 0)
].copy()

# 6. Traiter les descriptions manquantes
df["description"] = df["description"].fillna("UNKNOWN")

# 7. Calculer le chiffre d'affaires
df["total_amount"] = (df["quantity"] * df["unit_price"]).round(2)

# 8. Construire le schéma final
columns = [
    "transaction_id", "timestamp", "customer_id",
    "product_id", "description", "quantity",
    "unit_price", "total_amount", "country"
]
df = df[columns]

# 9. Export CSV
df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
df.to_csv(OUTPUT, index=False, encoding="utf-8")

# 10. Rapport qualité
report = {
    "source": "UCI Online Retail",
    "initial_rows": int(initial_rows),
    "final_rows": int(len(df)),
    "removed_rows": int(initial_rows - len(df)),
    "cancelled_invoices_detected": int(cancelled),
    "non_positive_quantity_detected": int(invalid_quantity),
    "non_positive_price_detected": int(invalid_price),
    "missing_customer_before_cleaning": int(missing_customer),
    "missing_description_before_cleaning": int(missing_description),
    "missing_customer_after_cleaning": int((df["customer_id"] == "").sum()),
    "missing_description_after_cleaning": int(df["description"].isna().sum()),
    "total_revenue": round(float(df["total_amount"].sum()), 2),
    "unique_products": int(df["product_id"].nunique()),
    "unique_customers": int(df.loc[df["customer_id"] != "", "customer_id"].nunique())
}

REPORT.write_text(json.dumps(report, indent=4), encoding="utf-8")

print("===== NETTOYAGE TERMINÉ =====")
print(json.dumps(report, indent=4))
print("\nCSV :", OUTPUT)
print("RAPPORT :", REPORT)
