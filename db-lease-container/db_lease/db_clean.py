import asyncio
import asyncpg
import logging
import os
import time

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


@run_function_as_async
def get_expired_resouces(db: firestore.Client):
    """
    Queries Firestore for all resources that are expired, but not ready
    to return to the pool
    """
    query = db.collection_group("resources").where(
        "expiry", "<", time.time()).where("status", "==", "leased")
    return query.stream()


@run_function_as_async
def set_status_to_ready(
            db: firestore.Client,
            db_type: str,
            db_size: str,
            resource_id: str):
    resource_ref = db.collection(db_type).document(db_size).collection(
            "resources").document(resource_id)
    resource_ref.update({"status": "ready"})


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
    if os.getenv("PROD") == 1:
        conn = await asyncpg.connect(
            host=f'/cloudsql/{resource_id}/.s.PGSQL.5432',
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
    # Connect with TCP when testing locally
    else:
        conn = await asyncpg.connect(
                host="127.0.0.1",
                port="5432",
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
        )
    # Drop the schema to drop all tables
    # (would probably be better to use a schema other than public)
    await conn.execute("DROP SCHEMA public CASCADE")
    logger.info(f"Dropped schema for db {DB_NAME} in instance {resource_id}")
    # Recreate the schema
    await conn.execute("CREATE SCHEMA public")
    logger.info(f"Recreated schema for db {DB_NAME} in instance {resource_id}")
    # Recreate the tables
    for statement in DDL_STATEMENTS:
        await conn.execute(statement)
    logger.info(f"Recreated tables for db {DB_NAME} in instance {resource_id}")
    await conn.close()


async def clean_instances(
        db: firestore.Client,
        logger: logging.Logger,
        interval: float = DB_CLEANUP_INTERVAL):
    """
    Periodically iterates through all expired resources which are unavailable
    and clears all tables.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            resources = await get_expired_resouces(db)
            for resource in resources:
                db_type = resource.get("database_type")
                db_size = resource.get("database_size")
                if db_type == "spanner":
                    await clean_spanner_instance(resource.id, logger)
                elif "cloud-sql" in db_type:
                    await clean_cloud_sql_instance(resource.id, logger)
                await set_status_to_ready(db, db_type,
                                          db_size, resource.id)
        except Exception:
            logger.exception("An error occured while clearing databases:")
