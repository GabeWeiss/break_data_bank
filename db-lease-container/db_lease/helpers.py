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

import re
import time
import asyncio
from functools import wraps, partial

DB_TYPES = {1: "cloud-sql", 2: "cloud-sql-read-replica", 3: "spanner"}
DB_SIZES = {1: "1x", 2: "2x", 3: "3x"}


def check_required_params(req, keys):
    """
    Helper to check if all valid params are present
    """
    for key in keys:
        if key not in req.keys():
            return False
    return True


def validate_resource_id(res_id):
    """
    Helper to check if resource_id value is valid
    """
    return True if re.match(r"^[a-z0-9-:]+$", res_id) else False


def validate_db_size(db_size):
    """
    Helper to check if database_size value is valid
    """
    return db_size in DB_SIZES.keys()


def validate_db_type(db_type):
    """
    Helper to check if database_type value is valid
    """
    return db_type in DB_TYPES.keys()


def is_available(resource):
    """
    Helper to check if resource is available.
    """
    return resource.get("status") == "ready"


def is_expired(resource):
    """
    Helper to check if resource is available.
    """
    return resource.get("expiry") < time.time()


def run_function_as_async(func):
    @wraps(func)
    async def wrapped_sync_function(*args, **kwargs):
        partial_func = partial(func, *args, **kwargs)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial_func)
    return wrapped_sync_function
