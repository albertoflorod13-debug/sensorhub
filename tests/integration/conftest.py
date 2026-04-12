import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient

_ROOT = Path(__file__).parent.parent.parent


def _load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


# Cargamos las vars antes de importar sensorhub para que Settings() las lea
_env = _load_env(_ROOT / ".env.test")
os.environ.update(_env)

from sensorhub.api import app  # noqa: E402

MONGO_URI = (
    f"mongodb://{_env['MONGO_USERNAME']}:{_env['MONGO_ROOT_PASSWORD']}"
    f"@{_env['MONGO_IP']}:{_env['MONGO_PORT']}"
)


@pytest.fixture(scope="session")
def docker_compose_file():
    return str(_ROOT / "docker-compose.test.yml")


@pytest.fixture(scope="session")
def docker_compose_project_name():
    return "sensorhub-test"


def _mongo_ok() -> bool:
    try:
        MongoClient(MONGO_URI, serverSelectionTimeoutMS=500).admin.command("ping")
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def client(docker_services):
    docker_services.wait_until_responsive(check=_mongo_ok, timeout=60, pause=1)
    return TestClient(app)


@pytest.fixture(autouse=True)
def clean_db(client):
    MongoClient(MONGO_URI)["sensorhub"]["sensor_data"].delete_many({})
    yield
