"""Création du client MongoDB."""

from pymongo import MongoClient
from pymongo.database import Database

from .config import Settings, get_settings


def get_client(settings: Settings | None = None) -> MongoClient:
    settings = settings or get_settings()
    # tz_aware : les dates relues sont des datetime UTC explicites.
    return MongoClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=settings.server_timeout_ms,
        tz_aware=True,
    )


def get_database(client: MongoClient | None = None, settings: Settings | None = None) -> Database:
    settings = settings or get_settings()
    client = client or get_client(settings)
    return client[settings.mongo_db]


def ping(client: MongoClient) -> bool:
    """Retourne True si le serveur répond, sans lever d'exception."""
    try:
        client.admin.command("ping")
        return True
    except Exception:
        return False
