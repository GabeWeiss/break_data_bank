
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

DB_TYPES = ["cloud-sql", "cloud-sql-read-replica", "spanner"]
DB_SIZES = ["1x", "2x", "3x"]


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
    return True if db_size in DB_SIZES else False


def validate_db_type(db_type):
    """
    Helper to check if database_type value is valid
    """
    return True if db_type in DB_TYPES else False


def is_available(resource):
    """
    Helper to check if resource is available.
    """
    # TODO: change this to inspect "clean" property to find available resource
    return resource.get("expiry") < time.time()
