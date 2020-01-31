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
from typing import Awaitable
from typing import Callable
from typing import List

import asyncpg
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.stackdriver import trace_exporter as stackdriver_exporter
import opencensus.trace.tracer as tracer

tracer = tracer.Tracer(
    exporter=stackdriver_exporter.StackdriverExporter(
        project_id="kvg-testing",
        transport=AsyncTransport
    ),
    sampler=tracer.samplers.AlwaysOnSampler()
)

ct = 0


async def cloud_sql_transaction(pool: asyncpg.pool):
    """Performs a simple transaction with the provided pool. """
    global ct
    with tracer.start_span("connection"):
        async with pool.acquire() as con:
            with tracer.start_span("transaction"):
                await con.fetch("SELECT 1")
            print("Transaction {} successful.".format(ct))
            ct += 1


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
        host="127.0.0.1",
        user="my-user",
        password="my-password",
        database="my_db",
        min_size=5,
        max_size=5,
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


if __name__ == "__main__":
    load = [
        (10, 30),  # 10s @ 30 qps
        (10, 60),  # 10s @ 60 qps
        (10, 90),  # 10s @ 90 qps
    ]
    asyncio.run(main(load))
