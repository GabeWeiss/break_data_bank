import argparse
import json
import re
import requests
import sys
import time

from google.cloud import firestore

import validate_transactions as validate
from utils_breaking import *

parser = argparse.ArgumentParser()
# TODO: Add ability to set seed and transfer directly to production
#parser.add_argument("-s", "--seed")
parser.add_argument("--validate_only")
args = parser.parse_args()

JOBS_FILENAME = "jobs_to_investigate.txt"
TMP_CACHED_DOC_NAME = "cached_staged"

if args.validate_only:
    success = validate_and_copy_data(JOBS_FILENAME, TMP_CACHED_DOC_NAME)
    if not success:
        print("There was a problem validating or moving the transactions. Check logs for more details.\n")
        sys.exit(1)
    print("Successfully validated and moved transactions.\n")
    sys.exit(0)


db = firestore.Client()

instance_keys = [
    "sql-1-1", "sql-1-2", "sql-1-3",
    "sql-2-1", "sql-2-2", "sql-2-3",
    "sql-3-1", "sql-3-2", "sql-3-3",
    "sqlrep-1-1", "sqlrep-1-2", "sqlrep-1-3",
    "sqlrep-2-1", "sqlrep-2-2", "sqlrep-2-3",
    "sqlrep-3-1", "sqlrep-3-2", "sqlrep-3-3",
    "spanner-1-1", "spanner-1-2", "spanner-1-3",
    "spanner-2-1", "spanner-2-2", "spanner-2-3",
    "spanner-3-1", "spanner-3-2", "spanner-3-3"
]

traffic_patterns = [
    "1-1", "1-2", "1-3", "2-1", "2-2", "2-3", "3-1", "3-2", "3-3"
]

JOBS_FILE = open(JOBS_FILENAME, "w", buffering=1)

def run_data(pattern, db_key):
    write_log(LOG_LVL_INFO, f" Running data for '{pattern}' against '{db_key}'")
    pattern = pattern.split("-")
    db_info = db_key.split("-")
    db_type = db_int_from_key(db_info[0])
    if db_type == -1:
        write_log(LOG_LVL_FATAL, "Couldn't get db_type. Unrecoverable error, ending run.")
        sys.exit(1)
    try:
        db_size = int(db_info[1])
        intensity = int(db_info[2])
    except:
        write_log(LOG_LVL_FATAL, "Couldn't get the db information properly. Unrecoverable error, ending run.")
        sys.exit(1)

    payload = {'read_pattern':int(pattern[0]),'write_pattern':int(pattern[1]),'db_type':db_type,'db_size':db_size,'intensity':intensity}
    headers = {'Content-Type':"application/json"}
    r = requests.post('https://breaking-orchestrator-abqdciqyya-uw.a.run.app/single', data=json.dumps(payload), headers=headers)
    try:
        response = json.loads(r.text)
    except:
        return None
    return response['jobs_id']

def wait_on_instance(ref):
    while True:
        time.sleep(3)
        resource = ref.get()
        if resource.to_dict()['status'] == "ready":
            break

warmed_instances = []
def warm_instance(instance_key, warm_id):
    global warmed_instances
    write_log(LOG_LVL_INFO, f" Warming instance: {instance_key}\n")
    run_data("1-1", instance_key)
    warmed_instances.append(warm_id)

def do_data_run():
    global warmed_instances

    write_log(LOG_LVL_INFO, "**** Starting data run ****")

    for key in instance_keys:
        x = re.match("^([a-z]+)\-([0-9]+)\-[0-9]+$", key)

        db_type = x.group(1)
        db_size = int(x.group(2))

        firestore_db_key = ""
        if db_type == "sql":
            firestore_db_key = "cloud-sql"
        elif db_type == "sqlrep":
            firestore_db_key = "cloud-sql-read-replica"
        elif db_type == "spanner":
            firestore_db_key = "spanner"
        else:
            write_log(LOG_LVL_FATAL, "Couldn't determine a firestore key from the instance_key, ending run.")
            sys.exit(1)

        db_size_key = firestore_size_key(db_size)

        resource_ref = db.collection("db_resources").document(firestore_db_key).collection("sizes").document(db_size_key).collection("resources").document(doc_name(db_type, db_size))

        traffic_base_ref = db.collection("events").document("next2020").collection(TMP_CACHED_DOC_NAME).document(key).collection("patterns")

        for pattern in traffic_patterns:
            traffic_ref = traffic_base_ref.document(pattern).collection("transactions")
            traffic = traffic_ref.limit(1).stream()

            if len(list(traffic)):
#                write_log(LOG_LVL_INFO, f"Already fetched pattern for {key} with pattern {pattern}")
                continue

            # Test if we've warmed up the instance or not as first runs
            # are always pretty ugly
            warmed_id = f"{db_type}-{db_size}"
            if not warmed_id in warmed_instances:
                warm_instance(key, warmed_id)
                wait_on_instance(resource_ref)


            run_result = run_data(pattern, key)
            write_log(LOG_LVL_INFO, f"Ran {key}, {pattern}: {run_result}")
            if run_result != None:
                JOBS_FILE.write(run_result)
#                output = f"./transfer.sh {key} {pattern} {run_result}\n"
#                print(f"{output}\n")

            wait_on_instance(resource_ref)

    write_log(LOG_LVL_INFO, "**** Finished data run ****")
    JOBS_FILE.close()


# Run for every combination of instance, size and pattern
do_data_run()

# Wait two minutes to ensure that the last data gets finally written.
# Sometimes there's a delay in Dataflow -> Firestore
time.sleep(120)

success = validate.validate_and_copy_data(JOBS_FILENAME, TMP_CACHED_DOC_NAME)
if not success:
    print("There were one or more problems validating the data set, check logs before proceeding.")
    sys.exit(1)


