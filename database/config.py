"""Paramètres de connexion lus depuis l'environnement.

Aucun identifiant n'est versionné : surcharger via les variables
d'environnement listées dans .env.example.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db: str
    server_timeout_ms: int


def get_settings() -> Settings:
    return Settings(
        mongo_uri=os.environ.get("MONGO_URI", "mongodb://localhost:27017/"),
        mongo_db=os.environ.get("MONGO_DB", "stocksense"),
        server_timeout_ms=int(os.environ.get("MONGO_TIMEOUT_MS", "3000")),
    )
