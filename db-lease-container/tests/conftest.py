import os

import pytest
from google.cloud import firestore
from main import app


@pytest.fixture(name="test_db", autouse=True)
def use_test_db(monkeypatch):
    test_credentials = os.getenv("TEST_PROJECT_CREDENTIALS")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", test_credentials)
    test_db = firestore.Client()
    yield test_db


@pytest.fixture()
def cleanup_db(test_db):
    pool_ref = (
        test_db.collection("db_resources")
        .document("cloud-sql")
        .collection("sizes")
        .document("1x")
        .collection("resources")
    )
    yield
    for resource in pool_ref.stream():
        resource.reference.delete()


@pytest.fixture(name="app", scope="function")
async def _app():
    await app.startup()
    yield app
    await app.shutdown()
