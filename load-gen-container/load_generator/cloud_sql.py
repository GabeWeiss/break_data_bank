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
from typing import Awaitable, Tuple
import asyncpg
import random
from .pubsub import PublishQueue

POOL_SIZE = 20

READ_STATEMENTS = ["SELECT 1"]


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


def read_transaction(pubQueue: PublishQueue, pool: asyncpg.pool) -> Awaitable:
    stmt = random.choice(READ_STATEMENTS)
    return run_transaction(pubQueue, pool, stmt)


async def run_transaction(pubQueue: PublishQueue, pool: asyncpg.pool, statement: str):
    """Performs a simple transaction with the provided pool. """
    loop = asyncio.get_running_loop()
    conn_start = loop.time()
    async with pool.acquire() as con:
        trans_start = loop.time()
        await con.fetch(statement)
        trans_end = loop.time()
    conn_stop = loop.time()
    await pubQueue.insert({
        "connection_start": conn_start,
        "transaction_start": trans_start,
        "transaction_end": trans_end,
        "job_id": "12345",
        "uuid": "12345"
    })
