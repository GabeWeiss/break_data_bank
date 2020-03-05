import os
import asyncio
import json
import logging
import time
from unittest import mock

import pytest

from main import app

PROJECT_ID = os.getenv("PROJECT_ID")


@pytest.fixture
def add_cloudsql_instance(test_db, app):
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
            "database_type": "cloud-sql",
            "database_size": "1x",
        }
    )


@pytest.mark.asyncio
async def test_cloud_sql_instance_is_cleaned(
    test_db, add_cloudsql_instance, cleanup_db, caplog
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
