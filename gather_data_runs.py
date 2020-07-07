import argparse
import json
import re
import requests
import sys
import time

from google.cloud import firestore

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--seed")
parser.add_argument("-p", "--project")
parser.add_argument("-r", "--region")
args = parser.parse_args()

db = firestore.Client()

JOBS_FILE = open("jobs_to_investigate.txt", "w")

def doc_name(t, s):
    project_name = "break-data-bank"
    if args.project:
        project_name = args.project
    region = "us-west2"
    if args.region:
        region = args.region
    if t == "sql":
        if s == 1:
            return f"{project_name}:{region}:break-sm1"
        elif s == 2:
            return f"{project_name}:{region}:break-med1"
        elif s == 3:
            return f"{project_name}:{region}:break-lrg1"
        else:
            return None
    elif t == "sqlrep":
        if s == 1:
            return f"{project_name}:{region}:break-sm1-r"
        elif s == 2:
            return f"{project_name}:{region}:break-med1-r"
        elif s == 3:
            return f"{project_name}:{region}:break-lrg1-r"
        else:
            return None
    elif t == "spanner":
        if s == 1:
            return "break-sm1"
        elif s == 2:
            return "break-med1"
        elif s == 3:
            return "break-lrg1"
        else:
            return None
    else:
        return None

def db_int_from_key(key):
    if key == "sql":
        return 1
    elif key == "sqlrep":
        return 2
    elif key == "spanner":
        return 3
    return -1

def run_data(pattern, db_key):
    print(f"Running data for '{pattern}' against '{db_key}'")
    pattern = pattern.split("-")
    db_info = db_key.split("-")
    db_type = db_int_from_key(db_info[0])
    if db_type == -1:
        print("Couldn't get db_type")
        sys.exit(1)
    try:
        db_size = int(db_info[1])
        intensity = int(db_info[2])
    except:
        print("Couldn't get the db information properly")
        sys.exit(1)

    payload = {'read_pattern':int(pattern[0]),'write_pattern':int(pattern[1]),'db_type':db_type,'db_size':db_size,'intensity':intensity}
    headers = {'Content-Type':"application/json"}
    r = requests.post('https://breaking-orchestrator-abqdciqyya-uw.a.run.app/single', data=json.dumps(payload), headers=headers)
    try:
        response = json.loads(r.text)
    except:
        return None
    return response['jobs_id']

def firestore_size_key(size_int):
    size_key = ""
    if size_int == 1:
        size_key = "1x"
    elif size_int == 2:
        size_key = "2x"
    elif size_int == 3:
        size_key = "4x"
    else:
        print("Couldn't determine a firestore key from the db_size")
        sys.exit(1)
    return size_key

def wait_on_instance(ref):
    while True:
        time.sleep(3)
        resource = ref.get()
        if resource.to_dict()['status'] == "ready":
            break

warmed_instances = []
def warm_instance(instance_key, warm_id):
    global warmed_instances
    print(f"\nWarming instance: {instance_key}\n")
    run_data("1-1", instance_key)
    warmed_instances.append(warm_id)


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

for key in instance_keys:
    x = re.match("^([a-z]+)\-([0-9]+)\-([0-9]+)$", key)

    db_type = x.group(1)
    db_size = int(x.group(2))
    intensity = int(x.group(3))

    firestore_db_key = ""
    if db_type == "sql":
        firestore_db_key = "cloud-sql"
    elif db_type == "sqlrep":
        firestore_db_key = "cloud-sql-read-replica"
    elif db_type == "spanner":
        firestore_db_key = "spanner"
    else:
        print("Couldn't determine a firestore key from the instance_key")
        sys.exit(1)

    db_size_key = firestore_size_key(db_size)

    resource_ref = db.collection("db_resources").document(firestore_db_key).collection("sizes").document(db_size_key).collection("resources").document(doc_name(db_type, db_size))

    traffic_base_ref = db.collection("events").document("next2020").collection("cached_1_6_12").document(key).collection("patterns")

    for pattern in traffic_patterns:
        traffic_ref = traffic_base_ref.document(pattern).collection("transactions")
        traffic = traffic_ref.limit(1).stream()

        if len(list(traffic)):
            print(f"Already fetched pattern for {key} with pattern {pattern}")
            continue

        # Test if we've warmed up the instance or not as first runs
        # are always pretty ugly
        warmed_id = f"{db_type}-{db_size}"
        if not warmed_id in warmed_instances:
            warm_instance(key, warmed_id)
            wait_on_instance(resource_ref)


        run_result = run_data(pattern, key)
        print(f"Ran {key}, {pattern}: {run_result}")
        if run_result != None:
            output = f"./transfer.sh {key} {pattern} {run_result}\n"
            JOBS_FILE.write(output)
            print(f"{output}\n")

        wait_on_instance(resource_ref)

JOBS_FILE.close()
