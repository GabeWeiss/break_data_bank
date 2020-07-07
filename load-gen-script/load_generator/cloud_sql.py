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

import asyncpg
import logging
import random
import re
import time
from typing import Awaitable, Tuple
import uuid

from . import db_limits
from .utils import Timer, OperationResults


logger = logging.getLogger(__name__)

inserted_count = 1

async def generate_transaction_args(
    host: str, port: int, database: str, user: str, password: str
) -> Tuple:
    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_size=1,
        max_size=20,
    )
    return (pool,)


READ_STATEMENTS = [
    "SELECT * from pictures",
    "SELECT colors.color FROM pictures JOIN colors ON pictures.fillColor=colors.id",
    "SELECT shapes1.shape, shapes2.shape FROM pictures JOIN shapes AS shapes1 ON pictures.shape1 = shapes1.id JOIN shapes AS shapes2 ON pictures.shape2 = shapes2.id",
    "SELECT color1.color AS lineColor, color2.color as fillColor, shapes1.shape AS shape1, shapes2.shape as shape2, artists.artist FROM pictures JOIN colors AS color1 ON pictures.lineColor = color1.id JOIN colors AS color2 on pictures.fillColor = color2.id JOIN shapes AS shapes1 ON pictures.shape1 = shapes1.id JOIN shapes AS shapes2 ON pictures.shape2 = shapes2.id JOIN artists ON pictures.artist = artists.id",
    "SELECT COUNT(*) FROM pictures"
    ]

def insert_new_row() -> str:
    return "INSERT INTO pictures (lineColor, fillColor, shape1, shape2, artist) VALUES ('{}', '{}', '{}', '{}', '{}')"

def update_lineColor() -> str:
    return "UPDATE pictures SET lineColor={} WHERE id={}"

def update_fillColor() -> str:
    return "UPDATE pictures SET fillColor={} WHERE id={}"

def update_shape1() -> str:
    return "UPDATE pictures SET shape1={} WHERE id={}"

def update_shape2() -> str:
    return "UPDATE pictures SET shape2={} WHERE id={}"

def update_artist() -> str:
    return "UPDATE pictures SET artist={} WHERE id={}"

UPDATE_COLOR = [
    update_lineColor(),
    update_fillColor()
]

UPDATE_SHAPE = [
    update_shape1(),
    update_shape2(),
]


def read_operation(pool: asyncpg.pool) -> Awaitable[OperationResults]:
    stmt = random.choice(READ_STATEMENTS)
    return perform_operation(pool, "read", stmt)

def write_operation(pool: asyncpg.pool) -> Awaitable[OperationResults]:
    global inserted_count
    rand = random.randint(1,10)
    if rand < 8:
        stmt = insert_new_row()
        inserted_count = inserted_count + 1
        return perform_operation(pool, "write", stmt.format(db_limits.random_color(), db_limits.random_color(), db_limits.random_shape(), db_limits.random_shape(), db_limits.random_artist()))
    elif rand == 8:
        stmt = random.choice(UPDATE_COLOR)
        return perform_operation(pool, "write", stmt.format(db_limits.random_color(), random.randint(1,inserted_count)))
    elif rand == 9:
        stmt = random.choice(UPDATE_SHAPE)
        return perform_operation(pool, "write", stmt.format(db_limits.random_shape(), random.randint(1,inserted_count)))
    elif rand == 10:
        stmt = update_artist()
        return perform_operation(pool, "write", stmt.format(db_limits.random_artist(), random.randint(1,inserted_count)))


async def perform_operation(
    pool: asyncpg.pool, operation: str, statement: str, timeout: float = 5
) -> OperationResults:
    global inserted_count
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
        logger.warning(f"Statement: {statement}")
        logger.warning("Transaction failed with exception: %s", ex)
        if inserted_count > 1 and re.search("INSERT", statement):
            inserted_count = inserted_count - 1

    return (
        operation,
        success,
        conn_timer.start,
        conn_timer.stop,
        trans_timer.start if trans_timer.start else conn_timer.stop,
        trans_timer.stop if trans_timer.stop else conn_timer.stop,
    )
