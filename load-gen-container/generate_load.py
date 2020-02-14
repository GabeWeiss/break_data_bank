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
import asyncpg
import concurrent.futures
from functools import partial
import getopt
from google.cloud import logging
import os
import sys
import time
from typing import Awaitable, Callable, List

# static vars
LOAD_INVALID        = -1
LOAD_LOW_FREQUENCY  = 1
LOAD_HIGH_FREQUENCY = 2
LOAD_SPIKEY         = 3

# These vars can/should be changed to suit environment
DB_PORT  = os.environ.get("DB_PORT", 5432) # default to Postgres default
DB_USER  = os.environ.get("DB_USER", None)
DB_PASS  = os.environ.get("DB_PASS", None)
DB_NAME  = os.environ.get("DB_NAME", None)
LOAD_PATTERN = os.environ.get("PATTERN", LOAD_INVALID)
POOL_MIN = 5
POOL_MAX = 5


fullCmdArguments = sys.argv
argumentList = fullCmdArguments[1:]
unixOptions = "hP:u:p:d:l:"
gnuOptions = ["help", "port=", "user=", "passwd=", "dbname=", "load="]

# probably don't NEED to do all this try/catch, but makes it easier to catch what/where goes wrong sometimes
# this chunk is just handling arguments
try:
    arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
except getopt.error as err:
    print (str(err))
    sys.exit(2)

for currentArgument, currentValue in arguments:
    if currentArgument in ("-h", "--help"):
        print ("\nusage: python generate_load.py [-h | -P port | -u user | -p passwd | -d dbname | -l load]\nOptions and arguments (and corresponding environment variables):\n-d db\t: database name to connect to\n-h\t: display this help\n-l load\t: Load pattern enumeration value\n-p pwd\t: password for the database user\n-P port\t:db port to connect to, defaults to 5432\n-u usr\t: database user to connect with\n\nOther environment variables:\nDB_USER\t: database user to connect with. Overridden by the -u flag\nDB_PASS\t: database password. Overridden by the -p flag.\nDB_NAME\t: database to connect to. Overridden by the -d flag.\nLOAD_PATTERN : Type of traffic load to generate. Overridden by the -l flag.\n    Values:\n     1: Consistent low frequency\n     2: Consistent high frequency\n     3: Spikey\n")
        sys.exit(0)

    if currentArgument in ("-d", "--dbname"):
        DB_NAME = currentValue
    elif currentArgument in ("-u", "--user"):
        DB_USER = currentValue
    elif currentArgument in ("-p", "--passwd"):
        DB_PASS = currentValue
    elif currentArgument in ("-P", "--port"):
        DB_PORT = currentValue
    elif currentArgument in ("-l", "--load"):
        LOAD_PATTERN = currentValue

# Validation check to see if we're good to proceed
if (DB_NAME == None or 
  DB_USER == None or
  DB_PASS == None or
  LOAD_PATTERN == LOAD_INVALID):
    print("\nInitialization failed, please be sure you're setting:\n DB_NAME,\n DB_USER,\n DB_PASS,\n LOAD_PATTERN\nin order to proceed.\n")
    sys.exit(2)



# These vars need to be consistent across all deployments
LOG_LATENCY = 'transaction-latency'
LOG_TRANSACTION_COUNT = 'transaction-count'

# Stackdriver init
logging_client = logging.Client()
log_name = LOG_LATENCY
logger = logging_client.logger(log_name)

transact_count = 0

# This handles the extra thread which sends the log messages to
# Stackdriver so it doesn't impact the time for the transaction to
# complete
executor = concurrent.futures.ProcessPoolExecutor(max_workers=3)

def send_log(latency):
    logger.log_text(str(latency))

async def cloud_sql_transaction(pool: asyncpg.pool):
    loop = asyncio.get_running_loop()
    """Performs a simple transaction with the provided pool. """
    global transact_count
    async with pool.acquire() as con:
        t = time.time()
        await con.fetch("SELECT 1")
        t = (time.time() - t) * 1000
        await loop.run_in_executor(executor, partial(send_log, latency=t))
        transact_count += 1
        print("Transaction {} successful. ({})".format(transact_count, t))


async def schedule_at(start_time: float, func: Callable[[], Awaitable]):
    """Helper function for calling a given function at a specific time."""
    now = asyncio.get_running_loop().time()
    await asyncio.sleep(start_time - now)
    await func()


def schedule_segment_load(
    start_time: float, duration: float, rate: float, func: Callable[[], Awaitable]
):
    """Schedules the func to be calls for the specified interval."""
    delta = 1.0 / rate  # gap between connections
    total_conns = round(duration / delta)  # total number of connections to send
    return [schedule_at(start_time + x * delta, func) for x in range(total_conns)]


async def main(load: List):
    pool = asyncpg.create_pool(
        host="127.0.0.1", # This doesn't change, we're using the proxy
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        min_size=POOL_MIN,
        max_size=POOL_MAX,
    )

    async with pool:
        scheduled_actions = []
        time = asyncio.get_running_loop().time()
        for seg in load:
            # Schedule connections that occur in the segment
            segment_load = schedule_segment_load(
                time, seg[0], seg[1], lambda: cloud_sql_transaction(pool)
            )
            # Add the segment load to our list of scheduled connections
            scheduled_actions.extend(segment_load)
            time += seg[0]

        # Wait for load to complete
        await asyncio.gather(*scheduled_actions)
        print("{} transactions completed.".format(len(scheduled_actions)))
    executor.shutdown() # this appears to be a no-op


if __name__ == "__main__":
    load = [
        (3, 30),  # 3s @ 30 qps
        (3, 60),  # 3s @ 60 qps
        (3, 90),  # 3s @ 90 qps
    ]
    asyncio.run(main(load))
