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
