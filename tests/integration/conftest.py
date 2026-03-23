import importlib
import os
import subprocess
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _load_env_file(path: str) -> dict:
    env = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


TEST_ENV = _load_env_file(".env.test")


def _wait_for_mongo():
    from pymongo import MongoClient

    for _ in range(30):
        try:
            MongoClient("mongodb://root:testpassword@localhost:27017", serverSelectionTimeoutMS=1000).admin.command("ping")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("MongoDB did not become ready in time")


def _wait_for_minio():
    from minio import Minio

    for _ in range(30):
        try:
            Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False).list_buckets()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("MinIO did not become ready in time")


@pytest.fixture(scope="session")
def docker_services():
    compose_env = {**os.environ, **TEST_ENV}
    subprocess.run(["docker", "compose", "up", "-d", "mongo", "minio"], env=compose_env, check=True)
    _wait_for_mongo()
    _wait_for_minio()
    yield
    subprocess.run(["docker", "compose", "down", "-v"], env=compose_env, check=True)


@pytest.fixture(scope="session")
def client(docker_services):
    os.environ.update(TEST_ENV)

    # Reload modules so db = MongoDB() uses the real connections, not any cached mocks
    import sensorhub.mongo
    import sensorhub.api
    importlib.reload(sensorhub.mongo)
    importlib.reload(sensorhub.api)

    from sensorhub.api import app
    return TestClient(app)


@pytest.fixture(autouse=True)
def clean_db(docker_services):
    from pymongo import MongoClient

    mongo = MongoClient("mongodb://root:testpassword@localhost:27017")
    mongo["sensorhub"]["sensor_data"].delete_many({})
    yield
