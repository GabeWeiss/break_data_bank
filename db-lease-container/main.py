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
import time
from functools import wraps, partial

from quart import Quart, request, jsonify
from google.cloud import firestore

from db_lease import helpers
from db_lease.helpers import DB_SIZES, DB_TYPES

app = Quart(__name__)

db = firestore.Client()


def run_function_as_async(func):
    @wraps(func)
    async def wrapped_sync_function(*args, **kwargs):
        partial_func = partial(func, *args, **kwargs)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial_func)
    return wrapped_sync_function


@run_function_as_async
@firestore.transactional
def lease(transaction, db_type, size, duration):
    """
    Finds the resource with the earliest expiry, and returns it if available.
    Returns None if no available resource is found.
    """
    pool_ref = db.collection(DB_TYPES[db_type]).document(DB_SIZES[size])
    query = pool_ref.collection("resources").order_by("expiry").limit(1)
    resources = query.stream(transaction=transaction)
    available = None
    for resource in resources:
        if helpers.is_available(resource):
            res_ref = pool_ref.collection("resources").document(resource.id)
            # TODO: set "clean" boolean to false here
            transaction.update(res_ref, {"expiry": time.time() + duration})
            available = resource
            break

    return available


@run_function_as_async
@firestore.transactional
def add(transaction, db_type, size, resource_id):
    """
    Adds a resource with the given id to the pool corresponding to the given
    database type and size if it doesn't already exist.
    """
    pool_ref = db.collection(DB_TYPES[db_type]).document(DB_SIZES[size])
    snapshot = pool_ref.collection("resources").document(resource_id).get(
            transaction=transaction)
    if not snapshot.exists:
        resource_ref = pool_ref.collection("resources").document(resource_id)
        transaction.set(resource_ref,
                        {"expiry": time.time() - 10})
    else:
        raise Exception(f"Resource {resource_id} already in pool")


@app.route('/isitworking', methods=['GET'])
@app.route('/', methods=['GET'])
def working():
    return "It's working", 200


@app.route('/lease', methods=['POST'])
async def lease_resource():
    """
    Route handler which takes database type, database size, and duration as
    parameters and leases a resource if available.
    """
    req_data = await request.get_json()

    if not helpers.check_required_params(
            req_data, ["database_type", "database_size", "duration"]):
        return "Bad Request: Missing required parameter", 400

    if not helpers.validate_db_type(req_data["database_type"]):
        return "Bad Request: Invalid database type", 400

    if not helpers.validate_db_size(req_data["database_size"]):
        return "Bad Request: Invalid database size", 400

    with db.transaction() as transaction:
        try:
            leased_resource = await lease(
                transaction,
                req_data["database_type"],
                req_data["database_size"],
                req_data["duration"])
        except Exception:
            app.logger.exception("Error occurred during transaction:")
            return f"Error occurred during transaction. See logs for info", 503

    if not leased_resource:
        app.logger.exception("All resources are currently in use")
        return "All resources are currently in use", 503

    response = {
        "resource_id": leased_resource.id,
        "expiration": leased_resource.get("expiry")
    }
    return jsonify(response), 200


@app.route('/add', methods=['POST'])
async def add_resource():
    """
    Route handler which takes database type, database size, and resource_id as
    parameters and adds a resource to the appropriate pool.
    """
    req_data = await request.get_json()
    if not helpers.check_required_params(
            req_data, ["database_type", "database_size", "resource_id"]):
        return "Bad Request: Missing required parameter", 400

    if not helpers.validate_db_type(req_data["database_type"]):
        return "Bad Request: Invalid database type", 400

    if not helpers.validate_db_size(req_data["database_size"]):
        return "Bad Request: Invalid database size", 400

    if not helpers.validate_resource_id(req_data["resource_id"]):
        return "Bad Request: Invalid resource_id", 400

    resource_id = req_data["resource_id"]
    with db.transaction() as transaction:
        try:
            await add(transaction,
                      req_data["database_type"],
                      req_data["database_size"],
                      resource_id)
            return f"Successfully added resource {resource_id} to pool", 200
        except Exception:
            app.logger.exception("Error occurred during transaction:")
            return f"Error occurred during transaction. See logs for info", 500


if __name__ == '__main__':
    app.run()
