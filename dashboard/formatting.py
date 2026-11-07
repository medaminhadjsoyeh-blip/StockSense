"""Mise en forme des nombres (convention française) et constantes d'affichage."""

from __future__ import annotations

# Codes UCI qui ne sont pas des articles : frais de port, ajustements manuels,
# frais bancaires... Ils faussent un classement « produits ».
NON_PRODUCT_CODES = (
    "POST", "DOT", "M", "m", "C2", "D", "S", "B", "CRUK", "PADS",
    "BANK CHARGES", "AMAZONFEE",
)

CURRENCY = "£"


def _group(value: float, decimals: int) -> str:
    text = f"{value:,.{decimals}f}"
    # 1,234,567.89 -> 1 234 567,89 (espace fine insécable)
    return text.replace(",", " ").replace(".", ",")


def money(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "—"
    return f"{_group(value, decimals)} {CURRENCY}"


def number(value: float | None, decimals: int = 0) -> str:
    if value is None:
        return "—"
    return _group(value, decimals)


def percent_delta(current: float, previous: float) -> str | None:
    """Variation relative pour st.metric ; None si la base est nulle."""
    if not previous:
        return None
    return f"{(current - previous) / previous * 100:+.1f} %".replace(".", ",")
