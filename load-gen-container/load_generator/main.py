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
from typing import Callable, Awaitable
import configargparse
from . import cloud_sql
from .pubsub import PublishQueue


async def delay_until(start_time: float, func: Callable[[], Awaitable]):
    """Helper function for calling a given function at a specific time."""
    now = asyncio.get_running_loop().time()
    await asyncio.sleep(start_time - now)
    await func()


def schedule_segment(
    start_time: float, duration: float, rate: float, func: Callable[[], Awaitable]
):
    """Schedules func() to be called a given rate over for given interval."""
    delta = 1.0 / rate  # gap between connections
    total_transactions = round(duration / delta)  # total number of transactions to run
    return [
        delay_until(start_time + x * delta, func) for x in range(total_transactions)
    ]


async def generate_load(args: configargparse.Namespace):
    # Create a queue for publishing results
    pubQueue = PublishQueue(args.pubsub_project, args.pubsub_topic)
    
    # TODO(kvg): configurable load parameters
    load = [
        (10, 500),  # 3s @ 30 qps
    ]

    # Set the read / write transactions to use
    read, write = None, None
    if args.target_type == "cloud-sql":
        args = await cloud_sql.generate_transaction_args(
            args.host, args.port, args.database, args.user, args.password
        )
        def read():
            cloud_sql.read_transaction(pubQueue, *args)
        # TODO(kvg) write patterns

    # Schedule the transactions to occur at the correct time.
    # Delay by 1s to ensure scheduling doesn't compete for thread resources.
    start_time = asyncio.get_running_loop().time() + 1.0
    cur_time = start_time
    transactions = []
    for seg in load:
        # Schedule connections that occur in the segment
        segment_load = schedule_segment(cur_time, seg[0], seg[1], read)
        # Add the segment load to our list of scheduled connections
        transactions.extend(segment_load)
        cur_time += seg[0]
    # Wait for all transactions to complete.
    await asyncio.gather(*transactions)
    end_time = asyncio.get_running_loop().time()

    ct, total_time = len(transactions), (end_time - start_time)
    print(f"{ct} transactions completed over {total_time}s. Avg: {ct/total_time} tps")
    
    await pubQueue.wait_for_close()


def main():
    parser = configargparse.ArgParser(default_config_files=["config.yaml"])
    parser.add_argument("-c", "--config", is_config_file=True)

    parser.add_argument("--target-type", required=True, choices=["cloud-sql"])
    parser.add_argument("--pubsub_project", required=True, help="pubsub project id")
    parser.add_argument("--pubsub_topic", required=True, help="pubsub topic id")

    cloud_sql_args = parser.add_argument_group("cloud-sql arguments")
    cloud_sql_args.add_argument(
        "--host", default="127.0.0.1", help="instance ip address"
    )
    cloud_sql_args.add_argument("--port", default=5432, type=int, help="instance port")
    cloud_sql_args.add_argument("-d", "--database", help="database name")
    cloud_sql_args.add_argument("-u", "--user", help="database user")
    cloud_sql_args.add_argument("-p", "--password", help="database user password")

    args = parser.parse_args()

    # Validate Cloud SQL flags
    if args.target_type == "cloud-sql":
        for flag in ["database", "user", "password"]:
            if getattr(args, flag) is None:
                parser.exit(1, f"--{flag} is required for cloud-sql targets\n")

    asyncio.run(generate_load(args))
