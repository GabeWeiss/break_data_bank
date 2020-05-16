import asyncio
import asyncpg
import logging
import os
import time

from google.cloud import firestore
from google.cloud import spanner

from .helpers import run_function_as_async

# TODO: These are env vars for now, will come up with a better solution later
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DDL_STATEMENTS = [
    "CREATE TABLE shapes ("
    "uuid VARCHAR(255) PRIMARY KEY,"
    "fillColor VARCHAR(255),"
    "lineColor VARCHAR(255),"
    "shape VARCHAR(255)"
    ");"
]

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
    resources = [r for r in query.stream()]

    return resources

@run_function_as_async
def get_down_resources(db: firestore.Client):
    """
    Queries Firestore for all resources that are expired, but not ready
    to return to the pool
    """

    query = (
        db.collection_group("resources")
        .where("status", "==", "down")
    )
    resources = [r for r in query.stream()]
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
def set_status_to_down(
    db: firestore.Client, db_type: str, db_size: str, resource_id: str
):
    pool_ref = (
        db.collection("db_resources")
        .document(db_type)
        .collection("sizes")
        .document(db_size)
        .collection("resources")
    )
    pool_ref.document(resource_id).update({"status": "down"})


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
    try:
        conn = await asyncpg.connect(**args,)
    except:
        print("Yeah no, couldn't connect to the db")
        return

    try:
        await conn.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        logger.info(f"Dropped db {DB_NAME} in {resource_id}")
        # Recreate the db
        await conn.execute(f"CREATE DATABASE {DB_NAME}")
        await conn.execute(f"\c {DB_NAME}")
        logger.info(f"Recreated db {DB_NAME} in {resource_id}")
        # Recreate the tables
        for statement in DDL_STATEMENTS:
            await conn.execute(statement)
        logger.info(f"Recreated tables for db {DB_NAME} in {resource_id}")
    finally:
        await conn.close()


async def create_cleanup_task(resource, db, logger):
    db_type = resource.get("database_type")
    db_size = resource.reference.parent.parent.id
    clean_func = {
        "spanner": clean_spanner_instance,
        "cloud-sql": clean_cloud_sql_instance,
    }[db_type]

    async def reset_resource():
        await clean_func(resource.id, logger)
        await set_status_to_ready(db, db_type, db_size, resource.id)

    return await reset_resource()


async def retry(db, resources, logger, interval=DB_CLEANUP_INTERVAL):
    while resources:
        if interval < MAX_RETRY_SECONDS:
            interval *= 2
        await asyncio.sleep(interval)

        tasks = []
        for resource in resources:
            task = create_cleanup_task(resource, db, logger)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        retry_resources = []
        for resource, result in zip(resources, results):
            if isinstance(result, Exception):
                # if fail
                log_msg = f"Failed to reset resource {resource.id,}. Will retry in {interval}s"
                logger.error(log_msg, exc_info=result)
                retry_resources.append(resource)
            else:
                # if success, set status to ready so it can go back in the pool
                db_type = resource.get("database_type")
                db_size = resource.reference.parent.parent.id
                await set_status_to_ready(db, db_type, db_size, resource.id)
        resources = retry_resources


async def clean_instances(db: firestore.Client, logger: logging.Logger):
    resources = await get_expired_resouces(db)
    tasks = [ await create_cleanup_task(r, db, logger) for r in resources ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    retry_resources = []
    for resource, result in zip(resources, results):
        db_type = resource.get("database_type")
        db_size = resource.reference.parent.parent.id
        if isinstance(result, Exception):
            await set_status_to_down(db, db_type, db_size, resource.id)
            retry_resources.append(resource)
    asyncio.create_task(retry(db, retry_resources, logger))


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
    while event.is_set():
        await asyncio.sleep(interval)
        await clean_instances(db, logger)
    event.set()
