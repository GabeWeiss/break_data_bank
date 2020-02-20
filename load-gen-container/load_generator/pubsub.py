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
import json
import multiprocessing as mp
import queue
import time
from typing import Any
from google.cloud import pubsub


class PublishQueue:
    """
    Asyncio-aware queue for delivering Google PubSub notifications in a separate process.
    """

    def __init__(self, project_id: str, topic_id: str):
        self._queue = mp.Queue()
        self._exit_event = mp.Event()
        self._process = mp.Process(target=self._start, args=(project_id, topic_id))
        self._process.start()

    async def insert(self, item: Any):
        await asyncio.get_running_loop().run_in_executor(None, self._queue.put_nowait, item)

    async def wait_for_close(self):
        await asyncio.get_running_loop().run_in_executor(None, self._join)

    def _start(self, project_id: str, topic_id: str):
        # Create the pubsub topic for creating
        publisher = pubsub.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        
        # Max amount of time between publishes
        INTERVAL = .5
        
        while True:  # run forever
            group = []
            end_time = time.time() + INTERVAL
            # group up notifications over INTERVAL amount of time
            while time.time() < end_time:
                try:
                    item = self._queue.get(block=True, timeout=INTERVAL)
                    group.append(item)
                except queue.Empty:
                    pass  # ignore errors from timeouts
            
            if len(group) > 0:
                publisher.publish(topic_path, data=json.dumps(group).encode('utf-8'))
            elif self._exit_event.is_set():
                # Exit if the flag is set and our work is finished
                break

    def _join(self):
        self._exit_event.set()
        self._process.join()
