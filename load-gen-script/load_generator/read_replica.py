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

import logging
import time
from typing import Awaitable, Tuple
import asyncpg
import random
from .utils import Timer, OperationResults


logger = logging.getLogger(__name__)

POOL_SIZE = 20
TIMEOUT = 5


async def generate_transaction_args(
    primary_host: str,
    replica_host: str,
    port: int,
    database: str,
    user: str,
    password: str,
) -> Tuple:
    primary_pool = await asyncpg.create_pool(
        host=primary_host,
        port=str(port),
        database=database,
        user=user,
        password=password,
        min_size=20,
        max_size=20,
    )
    replica_pool = await asyncpg.create_pool(
        host=replica_host,
        port=str(port),
        database=database,
        user=user,
        password=password,
        min_size=20,
        max_size=20,
    )
    return (
        primary_pool,
        replica_pool,
    )


READ_STATEMENTS = [
    "SELECT 1",
    "SELECT * from test_table",
    "SELECT column1 from test_table" "SELECT column2 from test_table WHERE column1=1",
]


def insert_new_row() -> str:
    return "INSERT INTO test_table (index, column1, column2, column3) VALUES ('{}', 1, 1, 1)".format(
        uuid.uuid4()
    )


def update_row() -> str:
    return "UPDATE test_table SET column1=column1*2 WHERE column1=1 "


WRITE_STATEMENTS = [
    insert_new_row,
    update_row,
]


def read_operation(
    primary_pool: asyncpg.pool, replica_pool: asyncpg.pool
) -> Awaitable[OperationResults]:
    stmt = random.choice(READ_STATEMENTS)
    pool = random.choice(
        primary_pool, replica_pool
    )  # split the read operations between the primary and replica dbs
    return perform_operation(pool, "read", stmt)


def write_operation(
    primary_pool: asyncpg.pool, replica_pool: asyncpg.pool
) -> Awaitable[OperationResults]:
    stmt = random.choice(write_STATEMENTS)
    pool = primary_pool  # all write operations go to the primary db
    return perform_operation(pool, "write", stmt)


async def perform_operation(
    pool: asyncpg.pool, operation: str, statement: str, timeout: float = 5
) -> OperationResults:
    """Performs a simple transaction with the provided pool. """
    success, conn_timer, trans_timer = True, Timer(), Timer()
    # Run the operation without letting it exceed the timeout given
    deadline = time.monotonic() + timeout
    try:
        with conn_timer:  # Start the connection timer
            time_left = deadline - conn_timer.start
            async with pool.acquire(timeout=time_left) as conn:
                with trans_timer:  # Start transaction timer
                    time_left = deadline - conn_timer.start
                    stmt = await conn.fetch(statement, timeout=time_left)
    except Exception as ex:
        success = False
        logger.warning("Transaction failed with exception: %s", ex)

    return (
        operation,
        success,
        conn_timer.start,
        conn_timer.stop,
        trans_timer.start if trans_timer.start else conn_timer.stop,
        trans_timer.stop if trans_timer.stop else conn_timer.stop,
    )
