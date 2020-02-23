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
from .pubsub import PublishQueue
from .utils import Timer, time_until


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


def read_transaction(pub_queue: PublishQueue, pool: asyncpg.pool) -> Awaitable:
    stmt = random.choice(READ_STATEMENTS)
    return run_transaction(pub_queue, pool, stmt)


async def run_transaction(
    pub_queue: PublishQueue, pool: asyncpg.pool, statement: str, timeout: float = 5
):
    """Performs a simple transaction with the provided pool. """
    connection, transaction = Timer(), Timer()
    # Run the operation without letting it exceed the timeout given
    deadline = time.monotonic() + timeout
    try:
        with connection:  # Start the connection timer
            time_left = deadline - connection.start
            async with pool.acquire(timeout=time_left) as conn:
                with transaction:  # Start transaction timer
                    time_left = deadline - transaction.start
                    await conn.fetch(statement, timeout=time_left)
    except Exception as ex:
        logger.warning("Transaction failed with exception: %s", ex)

    await pub_queue.insert(
        {
            "connection_start": connection.start,
            "connection_end": connection.stop,
            "transaction_start": transaction.start
            if transaction.start
            else connection.stop,
            "transaction_end": transaction.stop
            if transaction.stop
            else connection.stop,
            "job_id": "12345",
            "uuid": "12345",
        }
    )
