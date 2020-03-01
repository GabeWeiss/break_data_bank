import os

import pytest
from google.cloud import firestore


@pytest.fixture(name="test_db", autouse=True)
def use_test_db(monkeypatch):
    test_credentials = os.getenv("TEST_PROJECT_CREDENTIALS")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", test_credentials)
    test_db = firestore.Client()
    yield test_db


@pytest.fixture()
def cleanup_db(test_db):
    yield
    pool_ref = test_db.collection(
        "cloud-sql").document("1x").collection("resources")
    for resource in pool_ref.stream():
        resource.reference.delete()
