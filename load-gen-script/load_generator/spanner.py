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

from google.cloud import spanner

from .utils import Timer, OperationResults

logger = logging.getLogger(__name__)

READ_STATEMENTS = ["SELECT 1"]


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
def execute_statement(db_client: spanner.Database, statement: str, timeout: float):
    with db_client.snapshot() as snapshot:
        results = snapshot.execute_sql(statement, timeout=timeout)
        return results


async def generate_transaction_args(instance: str, database: str) -> Tuple:
    client = await get_database_client(instance, database)
    return (client,)


def read_operation(db_client: spanner.Database) -> Awaitable[OperationResults]:
    stmt = random.choice(READ_STATEMENTS)
    return perform_operation(db_client, "read", stmt)


async def perform_operation(
    db_client: spanner.Database, operation: str, statement: str, timeout: float = 5
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
            await execute_statement(db_client, statement, time_left)
    except Exception as ex:
        success = False
        logger.warning("Transaction failed with exception: %s", ex)

    return (
        operation,
        success,
        trans_timer.start,
        trans_timer.stop,
        trans_timer.start,
        trans_timer.stop,
    )
