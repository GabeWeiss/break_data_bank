from quart import Quart, request, jsonify, exceptions
from quart.helpers import make_response
from quart.flask_patch import abort
from google.cloud import firestore


from datetime import datetime, timedelta
import re
import pytz
from functools import wraps, partial
import asyncio

app = Quart(__name__)

db = firestore.Client()

DB_TYPES = ["cloud-sql", "cloud-sql-read-replica", "spanner"]
DB_SIZES = ["1x", "2x", "3x"]


def run_function_in_executor(func):
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
            abort(400)


def validate_resource_id(res_id):
    """
    Helper to check if resource_id value is valid
    """
    return res_id if re.match(r"^[a-z0-9-:]+$", res_id) else abort(400)


def validate_db_size(db_size):
    """
    Helper to check if database_size value is valid
    """
    return db_size if db_size in DB_SIZES else abort(400)


def validate_db_type(db_type):
    """
    Helper to check if database_type value is valid
    """
    return db_type if db_type in DB_TYPES else abort(400)


def is_available(resource):
    """
    Helper to check if resource is available.
    """
    # TODO: change this to inspect "clean" property to find available resource
    return resource.get("expiry") < datetime.now().replace(tzinfo=pytz.UTC)


@run_function_in_executor
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
            transaction.update(res_ref, {
                "expiry": datetime.now() + timedelta(seconds=duration)}
            )
            available = resource
            break

    return available


@run_function_in_executor
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
                        {"expiry": datetime.now() - timedelta(seconds=10)})
    else:
        raise exceptions.HTTPException(
            400, f"Resource {resource_id} already in pool", "Bad Request")


@app.route('/lease', methods=['POST'])
async def lease_resource():
    """
    Route handler which takes database type, database size, and duration as
    parameters and leases a resource if available.
    """
    req_data = await request.get_json()
    check_required_params(
        req_data, ["database_type", "database_size", "duration"])

    with db.transaction() as transaction:
        try:
            leased_resource = await lease(
                transaction,
                validate_db_type(req_data["database_type"]),
                validate_db_size(req_data["database_size"]),
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
        "expiration": leased_resource.get("expiry").timestamp()
    }
    return jsonify(response), 200


@app.route('/add', methods=['POST'])
async def add_resource():
    """
    Route handler which takes database type, database size, and resource_id as
    parameters and adds a resource to the appropriate pool.
    """
    req_data = await request.get_json()
    check_required_params(
        req_data, ["database_type", "database_size", "resource_id"])
    resource_id = validate_resource_id(req_data["resource_id"])
    with db.transaction() as transaction:
        try:
            await add(transaction,
                      validate_db_type(req_data["database_type"]),
                      validate_db_size(req_data["database_size"]),
                      resource_id)
            return f"Successfully added resource {resource_id} to pool", 200
        except Exception as e:
            err = await make_response(
                f"An error occurred during the transaction: {e}", 500)
            return err

app.run()
