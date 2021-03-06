import os
import asyncio
import json
import logging
import time
from unittest import mock

import pytest

PROJECT_ID = os.getenv("PROJECT_ID")


@pytest.fixture
def add_cloudsql_instance(test_db):
    pool_ref = (
        test_db.collection("db_resources")
        .document("cloud-sql")
        .collection("sizes")
        .document("1x")
        .collection("resources")
    )
    pool_ref.document(f"{PROJECT_ID}:us-west-1:test-db-1").set(
        {
            "expiry": time.time() - 10,
            "status": "ready",
            "connection_string": "127.0.0.1",
            "database_type": "cloud-sql",
        }
    )


@pytest.mark.asyncio
async def test_cloud_sql_instance_is_cleaned(
    app, test_db, add_cloudsql_instance, cleanup_db, caplog
):
    client = app.test_client()
    test_data = {"database_type": 1, "database_size": 1, "duration": 3}
    headers = {"Content-Type": "application/json"}
    with mock.patch("main.db", test_db), caplog.at_level(logging.INFO):
        response = await client.post(
            "/lease", data=json.dumps(test_data), headers=headers
        )
        resource_id = (await response.get_json())["resource_id"]
        pool_ref = (
            test_db.collection("db_resources")
            .document("cloud-sql")
            .collection("sizes")
            .document("1x")
            .collection("resources")
        )
        snapshot = pool_ref.document(resource_id).get()
        assert snapshot.get("status") == "leased"

        await asyncio.sleep(6)

        assert "Dropped schema" in caplog.text
        assert "Recreated schema" in caplog.text
        assert "Recreated tables" in caplog.text

        snapshot = pool_ref.document(resource_id).get()
        assert snapshot.get("status") == "ready"


@pytest.mark.asyncio
async def test_retry(app, test_db, add_cloudsql_instance, cleanup_db, caplog):
    client = app.test_client()
    test_data = {"database_type": 1, "database_size": 1, "duration": 3}
    headers = {"Content-Type": "application/json"}

    mock_clean_cloud_sql = mock.Mock(side_effect=Exception("Connection error"))
    with mock.patch("main.db", test_db), mock.patch(
        "db_lease.db_clean.clean_cloud_sql_instance", mock_clean_cloud_sql
    ), caplog.at_level(logging.ERROR):
        response = await client.post(
            "/lease", data=json.dumps(test_data), headers=headers
        )
        resource_id = (await response.get_json())["resource_id"]
        pool_ref = (
            test_db.collection("db_resources")
            .document("cloud-sql")
            .collection("sizes")
            .document("1x")
            .collection("resources")
        )
        snapshot = pool_ref.document(resource_id).get()
        assert snapshot.get("status") == "leased"

        await asyncio.sleep(20)

        assert "Will retry in 2s" in caplog.text
        assert "Will retry in 4s" in caplog.text
        assert "Will retry in 8s" in caplog.text

    pool_ref = (
        test_db.collection("db_resources")
        .document("cloud-sql")
        .collection("sizes")
        .document("1x")
        .collection("resources")
    )

    snapshot = pool_ref.document(resource_id).get()
    assert snapshot.get("status") == "down"
