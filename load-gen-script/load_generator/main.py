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
import configargparse
import functools
import logging
import math
import random
import time
from typing import Awaitable, Callable, List
import uuid

from . import cloud_sql, spanner, read_replica
from .pubsub import PublishQueue
from .utils import AsyncOperation


logger = logging.getLogger(__name__)

# These values copied from orchestrator code. Any changes need to be
# in both places. Todo: Move these to better place for both files
TRAFFIC_LOW = 1
TRAFFIC_HIGH = 2
TRAFFIC_SPIKEY = 3

CLOUD_SQL = 1
CLOUD_SQL_REPLICA = 2
CLOUD_SPANNER = 3

# Load patterns tuple are number of seconds, qps
LOAD_PATTERNS = {
    TRAFFIC_LOW : [(30, 10)],
    TRAFFIC_HIGH : [(30, 50)],
    TRAFFIC_SPIKEY : [(2, 60), (8, 10), (2, 60), (8,10), (2, 60), (8,10)]
}

async def schedule_at(start_time: float, func: Callable[[], Awaitable]):
    """Helper function for calling a given function at a specific time."""
    now = asyncio.get_running_loop().time()
    await asyncio.sleep(start_time - now)
    await func()


def schedule_segment(
    start_time: float, duration: float, rate: float, func: Callable[[], Awaitable]
) -> List[Awaitable]:
    """Schedules func() to be called a given rate over for given interval."""
    delta = 1.0 / rate  # gap between connections
    total_transactions = round(duration / delta)  # total number of transactions to run
    return [
        schedule_at(start_time + x * delta, func) for x in range(total_transactions)
    ]

def schedule_load(load: List[tuple], cur_time: float, func: Callable[[], Awaitable], transactions: List[Awaitable]):
    for seg in load:
        # Schedule connections that occur in the segment
        segment_load = schedule_segment(cur_time, seg[0], seg[1], func)
        # Add the segment load to our list of scheduled connections
        transactions.extend(segment_load)
        cur_time += seg[0]

def publish_results_from(
    pub_queue: PublishQueue, job_id: str, workload_id: str, operation: AsyncOperation
):
    """Function decorator to publish results after an operation is complete. """

    async def publish_results():
        results = await operation()
        op = results[0]
        success = results[1]
        con_start = results[2]
        con_end = results[3]
        trans_start = results[4]
        trans_end = results[5]

#        if success == False:
#            logger.info(f"\nOperation: {results[0]}\nConnection time: {results[3] - results[2]}\nTransaction time: {results[5]-results[4]}\n\n")

        await pub_queue.insert(
            {
                "workload_id": workload_id,
                "job_id": job_id,
                "operation": op,
                "success": success,
                "connection_start": con_start,
                "connection_end": con_end,
                "transaction_start": trans_start,
                "transaction_end": trans_end,
            }
        )

    return publish_results


async def generate_load(args: configargparse.Namespace):
    job_id = str(uuid.uuid4())

    # Set load patterns for read / write transations
    read_load = LOAD_PATTERNS[args.read_pattern]
    write_load = LOAD_PATTERNS[args.write_pattern]

    # Set the read / write transactions to use
    read, write = None, None
    if args.target_type == CLOUD_SQL:
        op_args = await cloud_sql.generate_transaction_args(
            args.connection_string, args.primary_port, args.database, args.user, args.password
        )
        read = functools.partial(cloud_sql.read_operation, *op_args)
        write = functools.partial(cloud_sql.write_operation, *op_args)

    elif args.target_type == CLOUD_SPANNER:
        op_args = await spanner.generate_transaction_args(args.connection_string, args.database)
        read = functools.partial(spanner.read_operation, *op_args)
        write = functools.partial(spanner.write_operation, *op_args)

    elif args.target_type == CLOUD_SQL_REPLICA:
        op_args = await read_replica.generate_transaction_args(
            args.connection_string,
            args.replica_ip,
            args.primary_port,
            args.replica_port,
            args.database,
            args.user,
            args.password,
        )
        read = functools.partial(read_replica.read_operation, *op_args)
        write = functools.partial(read_replica.write_operation, *op_args)

    # Use pubsub to publish the results of each operation
    pub_queue = PublishQueue(args.pubsub_project, args.pubsub_topic)
    read = publish_results_from(pub_queue, args.workload_id, job_id, read)
    write = publish_results_from(pub_queue, args.workload_id, job_id, write)

    # Convert from "off-the-wall" time to monotonic for consistency when timing
    delay = args.delay_until - time.monotonic()
    logger.info(f"Delaying load start until {args.delay_until} (~{delay:.4f}s)")

    # Schedule the transactions to start processing at the correct time
    start_time = args.delay_until
    transactions = []

    # Schedule transactions for both read and write operations
    schedule_load(read_load, args.delay_until, read, transactions)
    schedule_load(write_load, args.delay_until, write, transactions)

    # Wait for all transactions to complete.
    await asyncio.gather(*transactions)
    end_time = asyncio.get_running_loop().time()

    ct, total_time = len(transactions), (end_time - start_time)
    logger.info(
        f"{ct} transactions completed over {total_time}s. Avg: {ct/total_time} tps"
    )

    await pub_queue.wait_for_close()

def main():
    random.seed()

    parser = configargparse.ArgParser(default_config_files=["config.yaml"])
    parser.add_argument("-c", "--config", is_config_file=True)
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )

    parser.add_argument("-w", "--workload-id", help="uuid for the workload")

    parser.add_argument(
        "--target-type",
        required=True,
        type=int,
        choices=[CLOUD_SQL, CLOUD_SQL_REPLICA, CLOUD_SPANNER],
    )

    parser.add_argument(
        "--read-pattern",
        required=True,
        type=int,
        choices=[TRAFFIC_LOW, TRAFFIC_HIGH, TRAFFIC_SPIKEY],
    )

    parser.add_argument(
        "--write-pattern",
        required=True,
        type=int,
        choices=[TRAFFIC_LOW, TRAFFIC_HIGH, TRAFFIC_SPIKEY],
    )

    parser.add_argument("--pubsub_project", required=True, help="pubsub project id")
    parser.add_argument("--pubsub_topic", required=True, help="pubsub topic id")

    parser.add_argument(
        "--delay-until",
        help="Time since epoch to schedule load start. If unset, defaults to the "
        + "nearest whole second at least half a second into the future.",
        type=float,
    )
    parser.add_argument("-d", "--database", help="database name")
    
    parser.add_argument(
        "--connection_string", help="connection string. Cloud SQL will be an IP, Spanner will be a resource id"
    )

    parser.add_argument("--primary_port", default=5432, type=int, help="instance port for primary instance (Cloud SQL and Cloud SQL w/ replication")

    cloud_sql_args = parser.add_argument_group("cloud-sql arguments")
    
    cloud_sql_args.add_argument("-u", "--user", help="database user")
    cloud_sql_args.add_argument("-p", "--password", help="database user password")

    read_replica_args = parser.add_argument_group("read replica arguments")
    read_replica_args.add_argument(
        "--replica_ip", default="127.0.0.1", help="ip address for replica instance"
    )
    read_replica_args.add_argument("--replica_port", default=5432, type=int, help="instance port for replica instance")

    args = parser.parse_args()

    # Default to at least 1.5 into the future to account script start up time
    if not args.delay_until:
        # Use "time.time" for a consistent "off-the-wall" time between instances
        args.delay_until = math.ceil(time.time() + 0.5)
    # Switch to monotonic time from here on out for consistent timing
    args.delay_until = time.monotonic() + (args.delay_until - time.time())

    # Validate Cloud SQL flags
    if args.target_type == CLOUD_SQL:
        for flag in ["database", "user", "password", "connection_string"]:
            if getattr(args, flag) is None:
                parser.exit(1, f"--{flag} is required for cloud-sql targets\n")

    # Validate Spanner flags
    if args.target_type == CLOUD_SPANNER:
        for flag in ["database", "connection_string"]:
            if getattr(args, flag) is None:
                parser.exit(1, f"--{flag} is required for spanner targets\n")

    # Configure logging
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)7.7s] %(message)s")
    )
    root_logger = logging.getLogger("load_generator")
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO if not args.verbose else logging.DEBUG)

    root_logger.info(f"Starting run for:\n"
                     f" Database type:\t\t{args.target_type}\n"
                     f" Read pattern:\t\t{args.read_pattern}\n"
                     f" Write pattern:\t\t{args.write_pattern}\n"
                     f" Database name:\t\t{args.database}\n"
                     f" Connection string:\t{args.connection_string}\n"
                     f" Database user:\t\t{args.user}\n"
    )

    asyncio.run(generate_load(args))
