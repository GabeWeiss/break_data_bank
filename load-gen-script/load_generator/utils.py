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
import functools
import time
from typing import Awaitable, Callable, Optional, Tuple

# Convenience type for specifying return values of operation functions
# returns operation, success, conn_start, conn_end, transaction_start, transaction_end
OperationResults = Tuple[str, bool, float, float, float, float]

# Convenience type for async operation functions
AsyncOperation = Callable[[], Awaitable[OperationResults]]


class Timer:
    """Convenience class for safely measuring the execution time of operations."""

    def __init__(self):
        self.start: Optional[float] = None
        self.stop: Optional[float] = None

    def __enter__(self):
        if not self.start:
            self.start = time.monotonic()
        return self

    def __exit__(self, type_, value, traceback):
        if not self.stop:
            self.stop = time.monotonic()
