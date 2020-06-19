#! /usr/bin/python3

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from functools import partial, wraps
import logging
import random
import time
from typing import Awaitable, Tuple
import uuid

from google.cloud import spanner, spanner_admin_database_v1

from . import db_limits
from .utils import Timer, OperationResults

logger = logging.getLogger(__name__)

READ_STATEMENTS = [
    "SELECT * from pictures",
    "SELECT colors.color FROM pictures JOIN colors ON pictures.fillColor=colors.id",
    "SELECT shapes1.shape, shapes2.shape FROM pictures JOIN shapes AS shapes1 ON pictures.shape1 = shapes1.id JOIN shapes AS shapes2 ON pictures.shape2 = shapes2.id",
    "SELECT color1.color AS lineColor, color2.color as fillColor, shapes1.shape AS shape1, shapes2.shape as shape2, artists.artist FROM pictures JOIN colors AS color1 ON pictures.lineColor = color1.id JOIN colors AS color2 on pictures.fillColor = color2.id JOIN shapes AS shapes1 ON pictures.shape1 = shapes1.id JOIN shapes AS shapes2 ON pictures.shape2 = shapes2.id JOIN artists ON pictures.artist = artists.id",
    "SELECT COUNT(*) FROM pictures"
]


def insert_new_row() -> str:
    return "INSERT INTO pictures (id, lineColor, fillColor, shape1, shape2, artist) VALUES ('{}', {}, {}, {}, {}, {})"

def update_lineColor() -> str:
    return "UPDATE pictures SET lineColor={} WHERE fillColor={}"

def update_fillColor() -> str:
    return "UPDATE pictures SET fillColor={} WHERE lineColor={}"

def update_shape1() -> str:
    return "UPDATE pictures SET shape1={} WHERE shape2={}"

def update_shape2() -> str:
    return "UPDATE pictures SET shape2={} WHERE shape1={}"

def update_artist() -> str:
    return "UPDATE pictures SET artist={} WHERE artist={}"

UPDATE_COLOR = [
    update_lineColor(),
    update_fillColor()
]

UPDATE_SHAPE = [
    update_shape1(),
    update_shape2(),
]

def read_operation(
    db_client: spanner_admin_database_v1.types.Database,
) -> Awaitable[OperationResults]:
    stmt = random.choice(READ_STATEMENTS)
    return perform_operation(db_client, "read", stmt)

def write_operation(
    db_client: spanner_admin_database_v1.types.Database,
) -> Awaitable[OperationResults]:
    rand = random.randint(1,10)
    if rand < 8:
        stmt = insert_new_row()
        return perform_operation(db_client, "write", stmt.format(uuid.uuid4(), db_limits.random_color(), db_limits.random_color(), db_limits.random_shape(), db_limits.random_shape(), db_limits.random_artist()))
    elif rand == 8:
        stmt = random.choice(UPDATE_COLOR)
        return perform_operation(db_client, "write", stmt.format(db_limits.random_color(), db_limits.random_color()))
    elif rand == 9:
        stmt = random.choice(UPDATE_SHAPE)
        return perform_operation(db_client, "write", stmt.format(db_limits.random_shape(), db_limits.random_shape()))
    elif rand == 10:
        stmt = update_artist()
        return perform_operation(db_client, "write", stmt.format(db_limits.random_artist(), db_limits.random_artist()))


def run_function_as_async(func):
    @wraps(func)
    async def wrapped_sync_function(*args, **kwargs):
        partial_func = partial(func, *args, **kwargs)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial_func)

    return wrapped_sync_function


@run_function_as_async
def get_database_client(instance: str, database: str):
    return spanner.Client().instance(instance).database(database)


@run_function_as_async
def execute_read_statement(
    db_client: spanner_admin_database_v1.types.Database, statement: str, timeout: float
):
    with db_client.snapshot() as snapshot:
        results = []
        rows = snapshot.execute_sql(statement, timeout=timeout)
        for row in rows:
            results.append(row)
        return results


@run_function_as_async
def execute_write_statement(
    db_client: spanner_admin_database_v1.types.Database, statement: str, timeout: float
):
    def write_in_transaction(transaction):
        transaction.execute_update(statement)

    db_client.run_in_transaction(write_in_transaction, timeout_secs=timeout)


async def generate_transaction_args(instance: str, database: str) -> Tuple:
    client = await get_database_client(instance, database)
    return (client,)


async def perform_operation(
    db_client: spanner_admin_database_v1.types.Database,
    operation: str,
    statement: str,
    timeout: float = 5,
) -> OperationResults:
    """Performs a simple transaction with the provided pool. """
    success, trans_timer = (
        True,
        Timer(),
    )
    # Run the operation without letting it exceed the timeout given
    deadline = time.monotonic() + timeout
    try:
        with trans_timer:  # Start transaction timer
            time_left = deadline - trans_timer.start
            if operation == "read":
                results = await execute_read_statement(db_client, statement, time_left)
            elif operation == "write":
                results = await execute_write_statement(db_client, statement, time_left)
    except Exception as ex:
        success = False
        logger.warning(f"Statement: {statement}")
        logger.warning("Transaction failed with exception: %s", ex)

    return (
        operation,
        success,
        trans_timer.start,
        trans_timer.stop,
        trans_timer.start,
        trans_timer.stop,
    )
