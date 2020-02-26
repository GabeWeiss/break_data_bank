import asyncio
import re
import time
from functools import wraps, partial

from quart import Quart, request, jsonify
from quart.helpers import make_response
from google.cloud import firestore

app = Quart(__name__)

db = firestore.Client()

DB_TYPES = ["cloud-sql", "cloud-sql-read-replica", "spanner"]
DB_SIZES = ["1x", "2x", "3x"]


def run_function_as_async(func):
    @wraps(func)
    async def wrapped_sync_function(*args, **kwargs):
        partial_func = partial(func, *args, **kwargs)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial_func)
    return wrapped_sync_function


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


@run_function_as_async
@firestore.transactional
def lease(transaction, db_type, size, duration):
    """
    Finds the resource with the earliest expiry, and returns it if available.
    Returns None if no available resource is found.
    """
    pool_ref = db.collection(db_type).document(size)
    query = pool_ref.collection("resources").order_by("expiry").limit(1)
    resources = query.stream(transaction=transaction)
    available = None
    for resource in resources:
        if is_available(resource):
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
    pool_ref = db.collection(db_type).document(size)
    snapshot = pool_ref.collection("resources").document(resource_id).get(
            transaction=transaction)
    if not snapshot.exists:
        pool_ref = db.collection(db_type).document(size)
        resource_ref = pool_ref.collection("resources").document(resource_id)
        transaction.set(resource_ref,
                        {"expiry": time.time() - 10})
    else:
        raise Exception(f"Resource {resource_id} already in pool")


@app.route('/lease', methods=['POST'])
async def lease_resource():
    """
    Route handler which takes database type, database size, and duration as
    parameters and leases a resource if available.
    """
    req_data = await request.get_json()

    if not check_required_params(
            req_data, ["database_type", "database_size", "duration"]):
        return "Bad Request: Missing required parameter", 400

    if not validate_db_type(req_data["database_type"]):
        return "Bad Request: Invalid database type", 400

    if not validate_db_size(req_data["database_size"]):
        return "Bad Request: Invalid database size", 400

    with db.transaction() as transaction:
        try:
            leased_resource = await lease(
                transaction,
                req_data["database_type"],
                req_data["database_size"],
                req_data["duration"])
        except Exception as e:
            err = await make_response(
                f"An error occurred during the transaction: {e}", 400)
            return err

    if not leased_resource:
        err = await make_response("All resources are currently in use", 503)
        return err

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
    if not check_required_params(
            req_data, ["database_type", "database_size", "resource_id"]):
        return "Bad Request: Missing required parameter", 400

    if not validate_db_type(req_data["database_type"]):
        return "Bad Request: Invalid database type", 400

    if not validate_db_size(req_data["database_size"]):
        return "Bad Request: Invalid database size", 400

    if not validate_resource_id(req_data["resource_id"]):
        return "Bad Request: Invalid resource_id", 400

    resource_id = req_data["resource_id"]
    with db.transaction() as transaction:
        try:
            await add(transaction,
                      req_data["database_type"],
                      req_data["database_size"],
                      resource_id)
            return f"Successfully added resource {resource_id} to pool", 200
        except Exception as e:
            err = await make_response(
                f"An error occurred during the transaction: {e}", 500)
            return err

if __name__ == '__main__':
    app.run()
