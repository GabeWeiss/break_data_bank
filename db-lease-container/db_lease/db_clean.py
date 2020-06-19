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

# Change this to adjust how often the DB cleanup happens
DB_CLEANUP_INTERVAL = 120
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


async def clean_spanner_instance(resource_id: str, logger: logging.Logger):
    """
    Drops a database from the instance with the given resource_id and creates
    a new database with the same name and an identical schema
    """
    print("Starting the clean method")

    client = spanner.Client()

    instance = client.instance(resource_id)
    existing_db = instance.database(DB_NAME)
    # Drop the existing "dirty" database
    try:
        existing_db.drop()
    except Exception as ex:
        print("Wasn't able to drop the spanner databases")
        print(f"Error: {ex}")
        return False

    print("Dropped the db")
    logger.info(f"Dropped db {DB_NAME} from instance {DB_NAME}")

    # Create a new "clean" database with the same name
    try:
        f = open("db_lease/spanner_create_statements", "r")
        ddlStatements = f.read().strip().split("\n")
    except Exception as ex:
        print("Couldn't open our spanner create statements")
        print(f"Error: {ex}")
        return False

    print("read the DDL statements")
    op = instance.database(DB_NAME, ddlStatements).create()
    #op.result()    
    logger.info(f"Created db {DB_NAME} in instance {resource_id}")
    print("Created db")
    def insert_data(transaction):
        try:
            f = open("db_lease/spanner_insert_statements", "r")
            dmlStatements = f.read().strip().split("\n")
        except Exception as ex:
            print("Couldn't open our spanner insertion statements")
            print(f"Error: {ex}")
            return False
        row_ct = transaction.batch_update(dmlStatements)
        print(row_ct)

    new_db = instance.database(DB_NAME)
    new_db.run_in_transaction(insert_data)
    return True

async def clean_cloud_sql_instance(resource_id: str, logger: logging.Logger):
    # Intentionally connecting to the postgres system DB here
    # because we can't drop the database when it's the active
    # one. Which means we'll do this, then you'll see a close
    # and connect again, which we need in order to create the
    # tables in the correct db
    args = {
        "host": "127.0.0.1",
        "port": "5432",
        "database": "postgres",
        "user": DB_USER,
        "password": DB_PASSWORD,
    }

    # If we're in our production environment, connect to
    # the "correct" DB instead of our localhost, which is
    # manually setup cloud sql proxy pointing at a test
    # instances to play on
    if os.getenv("PROD"):
        args["host"] = f"/cloudsql/{resource_id}/.s.PGSQL.5432"
        del args["port"]

    # Here's the connection to the postgres db
    try:
        conn = await asyncpg.connect(**args,)
    except Exception as ex:
        print("Yeah no, couldn't connect to the postgres db")
        print("Error connecting: %s", ex)
        return

    # Dropping and re-creating a clean db
    # Note, closing current connection at the
    # end of it
    try:
        await conn.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        logger.info(f"Dropped db {DB_NAME} in {resource_id}")
        # Recreate the db
        await conn.execute(f"CREATE DATABASE {DB_NAME}")
        logger.info(f"Recreated db {DB_NAME} in {resource_id}")
    except Exception as ex:
        print("Couldn't drop and recreate the database")
        print("Error dropping: %s", ex)
        return False
    finally:   
        await conn.close()

    # And here's connecting to the DB we just created
    try:
        args['database'] = DB_NAME
        conn = await asyncpg.connect(**args,)
    except Exception as ex:
        print(f"Yeah no, couldn't connect to the {DB_NAME} db")
        print("Error connecting: %s", ex)
        return False

    try:
        # Recreate the tables
        f = open("./db_lease/sql_create_statements", "r")
        for statement in f:
            await conn.execute(statement)
        logger.info(f"Recreated tables for db {DB_NAME} in {resource_id}")
    except Exception as ex:
        print("Wasn't able to re-create our tables")
        print("Error: %s", ex)
        return False
    finally:
        await conn.close()

    return True

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
    success = True
    resources = await get_expired_resouces(db)
    for resource in resources:
        db_type = resource.get("database_type")
        db_size = resource.reference.parent.parent.id
        if db_type == "cloud-sql" or db_type == "cloud-sql-read-replica":
            success = await clean_cloud_sql_instance(resource.id, logger)
        elif db_type == "spanner":
            success = await clean_spanner_instance(resource.id, logger)
        else:
            print(f"WTF YOU SEND ME?! I DON'T UNDERSTAND: {db_type}")
            return
        if success:
            await set_status_to_ready(db, db_type, db_size, resource.id)

    return



    retry_resources = []
    print("Starting to clean")
    for resource, result in zip(resources, results):
        db_type = resource.get("database_type")
        db_size = resource.reference.parent.parent.id
        if isinstance(result, Exception):
            await set_status_to_down(db, db_type, db_size, resource.id)
            retry_resources.append(resource)
    print("Finished first attempt, moving to retry instances")
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
