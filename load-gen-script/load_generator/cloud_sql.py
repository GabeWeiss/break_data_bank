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
    host: str, port: int, database: str, user: str, password: str
) -> Tuple:
    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_size=20,
        max_size=20,
    )
    return (pool,)


READ_STATEMENTS = ["SELECT 1"]


def read_operation(pool: asyncpg.pool) -> Awaitable[OperationResults]:
    stmt = random.choice(READ_STATEMENTS)
    return perform_operation(pool, "read", stmt)


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
