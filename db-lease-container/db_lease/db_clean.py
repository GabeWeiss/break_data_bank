import asyncio
import asyncpg
import functools
import logging
import os
import time
from typing import Callable

from google.cloud import firestore
from google.cloud import spanner

from .helpers import run_function_as_async

# TODO: what does a clean database schema look like?
DDL_STATEMENTS = []

# TODO: These are env vars for now, will come up with a better solution later
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Change this to adjust how often the DB cleanup happens
DB_CLEANUP_INTERVAL = 1
MAX_RETRY_SECONDS = 120


@run_function_as_async
def get_expired_resouces(db: firestore.Client):
    """
    Queries Firestore for all resources that are expired, but not ready
    to return to the pool
    """

    query = (
        db.collection_group("resources")
        .where("expiry", "<", time.time())
        .where("status", "==", "leased")
    )
    resources = []
    for resource in query.stream():
        resources.append(resource)
    return resources


@run_function_as_async
def set_status_to_ready(
    db: firestore.Client, db_type: str, db_size: str, resource_id: str
):
    pool_ref = (
        db.collection("db_resources")
        .document(db_type)
        .collection("sizes")
        .document(db_size)
        .collection("resources")
    )
    pool_ref.document(resource_id).update({"status": "ready"})


@run_function_as_async
def clean_spanner_instance(resource_id: str, logger: logging.Logger):
    """
    Drops a database from the instance with the given resource_id and creates
    a new database with the same name and an identical schema
    """
    with spanner.Client() as client:
        instance = client.instance(resource_id)
        # Drop the existing "dirty" database
        op = instance.database(DB_NAME).drop()
        op.result()
        logger.info(f"Dropped db {DB_NAME} from instance {DB_NAME}")
        # Create a new "clean" database with the same name
        op = instance.database(DB_NAME, DDL_STATEMENTS).create()
        op.result()
        logger.info(f"Created db {DB_NAME} in instance {resource_id}")


async def clean_cloud_sql_instance(resource_id: str, logger: logging.Logger):
    args = {
        "host": "127.0.0.1",
        "port": "5432",
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    if os.getenv("PROD"):
        args["host"] = f"/cloudsql/{resource_id}/.s.PGSQL.5432"
        del args["port"]
    conn = await asyncpg.connect(**args,)
    try:
        await conn.execute("DROP SCHEMA public CASCADE")
        logger.info(f"Dropped schema for db {DB_NAME} in {resource_id}")
        # Recreate the schema
        await conn.execute("CREATE SCHEMA public")
        logger.info(f"Recreated schema for db {DB_NAME} in {resource_id}")
        # Recreate the tables
        for statement in DDL_STATEMENTS:
            await conn.execute(statement)
        logger.info(f"Recreated tables for db {DB_NAME} in {resource_id}")
    finally:
        await conn.close()


async def reset_resource(
    db: firestore.Client, db_type: str, db_size: str, resource_id: str, func: Callable,
):
    await func()
    await set_status_to_ready(db, db_type, db_size, resource_id)


async def clean_instances(db: firestore.Client, logger: logging.Logger):
    try:
        resources = await get_expired_resouces(db)
        tasks = []
        for resource in resources:
            db_type = resource.get("database_type")
            db_size = resource.reference.parent.parent.id
            clean_func = {
                "spanner": clean_spanner_instance,
                "cloud-sql": clean_cloud_sql_instance,
            }[db_type]
            clean_partial = functools.partial(clean_func, resource.id, logger)
            tasks.append(
                reset_resource(db, db_type, db_size, resource.id, clean_partial)
            )
        asyncio.gather(*tasks)

    except Exception:
        logger.exception("An error occured while clearing databases:")


async def loop_clean_instances(
    db: firestore.Client,
    logger: logging.Logger,
    event: asyncio.Event,
    interval: float = DB_CLEANUP_INTERVAL,
):
    """
    Periodically iterates through all expired resources which are unavailable
    and clears all tables.
    """
    retry_seconds = DB_CLEANUP_INTERVAL
    while event.is_set():
        await asyncio.sleep(interval)
        if retry_seconds < MAX_RETRY_SECONDS:
            try:
                await clean_instances(db, logger)
            except Exception:
                logger.exception("An error occured while clearing databases:")
                retry_seconds *= 2
                if retry_seconds < MAX_RETRY_SECONDS:
                    logger.warning(f"Will retry in {retry_seconds} seconds")
            else:
                # if a successful operation occurs, reset exponential backoff
                retry_seconds = DB_CLEANUP_INTERVAL
    event.set()
