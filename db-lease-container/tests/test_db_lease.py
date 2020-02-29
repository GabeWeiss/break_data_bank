import os
import json
import time
from unittest import mock


import pytest
from google.cloud import firestore

from main import app

test_app = app.test_client()


@pytest.fixture(name="test_db", autouse=True)
def use_test_db(monkeypatch):
    test_credentials = os.getenv("TEST_PROJECT_CREDENTIALS")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", test_credentials)
    test_db = firestore.Client()
    yield test_db


@pytest.fixture()
def resource_available(test_db):
    pool_ref = (
        test_db.collection("db_resources")
        .document("cloud-sql").collection("sizes")
        .document("1x")
        .collection("resources")
    )
    pool_ref.add({"expiry": time.time() - 10, "status": "ready"})
    yield
    for resource in pool_ref.stream():
        resource.reference.delete()


@pytest.fixture()
def resource_unavailable(test_db):
    pool_ref = (
        test_db.collection("db_resources")
        .document("cloud-sql")
        .collection("sizes")
        .document("1x")
        .collection("resources")
    )
    pool_ref.add({"expiry": time.time() + 3600, "status": "leased"})
    yield
    for resource in pool_ref.stream():
        resource.reference.delete()


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


@pytest.mark.asyncio
async def test_add_resource_to_pool(test_db, cleanup_db):
    client = app.test_client()
    test_data = {
        "resource_id": "test-project:us-west2:test-instance",
        "database_type": 1,
        "database_size": 1,
    }
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db):
        response = await client.post(
            "/add", data=json.dumps(test_data), headers=headers
        )
    resp_data = await response.get_data()
    assert "Successfully added resource".encode() in resp_data
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_add_resource_already_exists(test_db):
    client = app.test_client()
    test_data = {
        "resource_id": "test-project:us-west2:test-instance",
        "database_type": 1,
        "database_size": 1,
    }
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db):
        await client.post("/add", data=json.dumps(test_data), headers=headers)
        response = await client.post(
            "/add", data=json.dumps(test_data), headers=headers
        )
    resp_data = await response.get_data()
    assert "Error occurred during transaction".encode() in resp_data
    assert response.status_code == 500


@pytest.mark.asyncio
async def test_lease_resource_when_available(test_db, resource_available):
    client = app.test_client()
    test_data = {"database_type": 1, "database_size": 1, "duration": 300}
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db):
        response = await client.post(
            "/lease", data=json.dumps(test_data), headers=headers
        )
    leased_resource = await response.get_data()
    assert "resource_id".encode() in leased_resource
    assert "expiration".encode() in leased_resource
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_lease_resource_when_unavailable(test_db, resource_unavailable):
    client = app.test_client()
    test_data = {"database_type": 1, "database_size": 1, "duration": 300}
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db):
        response = await client.post(
            "/lease", data=json.dumps(test_data), headers=headers
        )
    resp_data = await response.get_data()
    assert "All resources are currently in use".encode() in resp_data
    assert response.status_code == 503


@pytest.mark.asyncio
async def test_add_resource_logs_exceptions(test_db, caplog):
    client = app.test_client()
    test_data = {
        "resource_id": "test-project:us-west2:test",
        "database_type": 1,
        "database_size": 1,
    }
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db):
        await client.post("/add", data=json.dumps(test_data), headers=headers)
        await client.post("/add", data=json.dumps(test_data), headers=headers)
    assert "test-project:us-west2:test already in pool" in caplog.text


@pytest.mark.asyncio
async def test_lease_resource_logs_exceptions(test_db, caplog):
    client = app.test_client()
    test_data = {"database_type": 1, "database_size": 1, "duration": 300}
    headers = {"Content-Type": "application/json"}

    mock_lease = mock.Mock(
        side_effect=Exception("Transaction failed! Please try again.")
    )
    with mock.patch("main.db", test_db), mock.patch("main.lease", mock_lease):
        await client.post(
            "/lease", data=json.dumps(test_data), headers=headers)
    assert "Transaction failed! Please try again." in caplog.text
