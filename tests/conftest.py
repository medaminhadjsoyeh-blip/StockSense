import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "data"))
sys.path.insert(0, str(ROOT / "scripts"))

from database.client import get_client, ping  # noqa: E402
from database.config import Settings  # noqa: E402

TEST_URI = os.environ.get("MONGO_TEST_URI", "mongodb://localhost:27017/")


@pytest.fixture(scope="session")
def mongo_client():
    client = get_client(Settings(mongo_uri=TEST_URI, mongo_db="unused", server_timeout_ms=1500))
    if not ping(client):
        pytest.skip(f"MongoDB indisponible sur {TEST_URI} (docker compose up -d)")
    yield client
    client.close()


@pytest.fixture
def mongo_db(mongo_client):
    """Base réelle jetable, supprimée après le test."""
    name = f"stocksense_test_{uuid.uuid4().hex[:8]}"
    yield mongo_client[name]
    mongo_client.drop_database(name)


@pytest.fixture
def db(mongo_db):
    # mongomock diverge de pymongo récent (bulk_write) : on teste sur un vrai serveur.
    return mongo_db


@pytest.fixture(scope="session")
def sample_csv(tmp_path_factory):
    import generate_sample

    path = tmp_path_factory.mktemp("sample") / "transactions.csv"
    generate_sample.main(["--rows", "1500", "--seed", "11", "--out", str(path)])
    return path
