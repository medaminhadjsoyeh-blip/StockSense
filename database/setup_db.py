"""Crée les collections, validateurs et index MongoDB (idempotent).

Usage : python -m database.setup_db
"""

import sys

from .client import get_client, get_database, ping
from .config import get_settings
from .repository import ensure_schema


def main() -> int:
    settings = get_settings()
    client = get_client(settings)
    if not ping(client):
        print(f"MongoDB injoignable : {settings.mongo_uri}", file=sys.stderr)
        return 1
    db = get_database(client, settings)
    for name, indexes in ensure_schema(db).items():
        print(f"{name:<20} index : {', '.join(indexes)}")
    print(f"\nSchéma appliqué sur la base '{settings.mongo_db}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
