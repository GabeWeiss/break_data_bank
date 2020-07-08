import argparse
import json
import numpy as np
import re
import requests
from scipy import stats
import sys
import time

from google.cloud import firestore

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--seed")
parser.add_argument("-p", "--project")
parser.add_argument("-r", "--region")
args = parser.parse_args()

db = firestore.Client()

JOBS_FILENAME = "jobs_to_investigate.txt"
JOBS_FILE = open(JOBS_FILENAME, "w")
TMP_CACHED_DOC_NAME = "cached_staged"
PRODUCTION_CACHED_NAME = "cached"

# Validation variables
TRANSACTION_COUNT_MINIMUM = 40
# In milliseconds
MINIMUM_TIME_LENGTH = 20000
# Expressed as a percent over the average
LATENCY_Z_SCORE_THRESHOLD = 8

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

# I do it this way, instead of validating per job because it takes
# some time to finalize, and I want to be sure the last transactions
# are pulled in from Dataflow
x = datetime.datetime.now()
LOGS_FILE = open(f'logs/log_{x.strftime("%Y_%m_%d")}', "a")
LOG_LVL_INFO = " - [INFO ] - "
LOG_LVL_WARNING = " - [WARN ] - "
LOG_LVL_FATAL = " - [FATAL] - "

def writeLog(log_level, msg):
    LOGS_FILE.write(f'{datetime.datetime.now().strftime("%H:%M:%S")}{log_level}{msg}\n')

def db_key(db_type, db_size, intensity):
    if db_type == 1:
        return f"sql-{db_size}-{intensity}"
    elif db_type == 2:
        return f"sqlrep-{db_size}-{intensity}"
    elif db_type == 3:
        return f"spanner-{db_size}-{intensity}"

def validate_and_copy_data():
    writeLog(LOG_LVL_INFO, "**** Beginning transactions validation ****")
    success = True

    JOBS_FILE = open(JOBS_FILENAME, "r")
    for job_id in JOBS_FILE:
        job_id = job_id[:-1] # strip off newline character
        writeLog(LOG_LVL_INFO, f" Validating job '{job_id}'")
        job_reference = db.collection("events").document("next2020").collection("jobs").document(job_id)
        job = job_reference.get()
        job_d = job.to_dict()
        try:
            db_size = job_d['db_size']
            db_type = job_d['db_type']
            read_pattern = job_d['read_pattern']
            write_pattern = job_d['write_pattern']
            intensity = job_d['read_intensity'] # read/write intensity is the same
        except:
            writeLog(LOG_LVL_WARNING, f"Didn't have the data for the job in Firestore: {job_id}")
            continue

        transactions_ref = db.collection("events").document("next2020").collection("transactions").document(job_id).collection("transactions")
        transactions = list(transactions_ref.stream())

        # Used to determine if we have a minimum time threshold
        start_timestamp = 999999999999999999
        end_timestamp = -1

        # This looks for outliers that would negate/invalidate a data set
        latencies = []
        transaction_count = len(transactions)
        if transaction_count < TRANSACTION_COUNT_MINIMUM:
            writeLog(LOG_LVL_WARNING, f"DB KEY: {key}, PATTERN: {pattern} does not meet minimum transaction count threshold")
            success = False

        for t in transactions:
            t_dict = t.to_dict()
            try:
                transaction_time = t_dict['timestamp']
            except:
                writeLog(LOG_LVL_FATAL, f"Transaction '{t.id}' in job '{job_id}' has no timestamp, ending validation.")
                return False
            if transaction_time < start_timestamp:
                start_timestamp = transaction_time
            elif transaction_time > end_timestamp:
                end_timestamp = transaction_time

            latency = t_dict['average_latency']
            latencies.append(latency)

        series_time = end_timestamp - start_timestamp
        if (series_time) < MINIMUM_TIME_LENGTH:
            writeLog(LOG_LVL_WARNING, f"DB KEY: {key}, PATTERN: {pattern} didn't run for long enough. Only ran for {series_time} seconds.")
            success = False

        # Usin the z-score to determine outliers
        # Fair warning, I do not fully understand outliers/statistics
        # so this is "best guess" of tweaking the z-score threshold by
        # using a known good data set as a sample to set my threshold
        # to where the current set passes, such that future sets with large
        # outliers should fail
        mean_y = np.mean(latencies)
        stdev_y = np.std(latencies)
        z_scores = [(y - mean_y) / stdev_y for y in latencies]
        a = np.where(np.abs(z_scores) > LATENCY_Z_SCORE_THRESHOLD)
        if len(a[0]) > 0:
            writeLog(LOG_LVL_WARNING, f"DB KEY: {key}, PATTERN: {pattern} appears to have outliers")
            success = False

        writeLog(LOG_LVL_INFO, f"  {transaction_count} entries validated")

        if success:
            cache_ref = db.collection("events").document("next2020").collection(TMP_CACHED_DOC_NAME).document(db_key).collection("patterns").document(f"{read_pattern}-{write_pattern}").collection("transactions")
            writeLog(LOG_LVL_INFO, f" Moving transactions to staging cache")
            for t in transactions:
                t_dict = t.to_dict()
                cache_ref.add(t_dict)



    JOBS_FILE.close()
    return success

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
    writeLog(LOG_LVL_INFO, f" Running data for '{pattern}' against '{db_key}'")
    pattern = pattern.split("-")
    db_info = db_key.split("-")
    db_type = db_int_from_key(db_info[0])
    if db_type == -1:
        writeLog(LOG_LVL_FATAL, "Couldn't get db_type. Unrecoverable error, ending run.")
        sys.exit(1)
    try:
        db_size = int(db_info[1])
        intensity = int(db_info[2])
    except:
        writeLog(LOG_LVL_FATAL, "Couldn't get the db information properly. Unrecoverable error, ending run.")
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
        writeLog(LOG_LVL_FATAL, "Couldn't determine a firestore key from the db_size. Unrecoverable error, ending run.")
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
    writeLog(LOG_LVL_INFO, f" Warming instance: {instance_key}\n")
    run_data("1-1", instance_key)
    warmed_instances.append(warm_id)


def do_data_run():
    global warmed_instances

    writeLog(LOG_LVL_INFO, "**** Starting data run ****")

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
            writeLog(LOG_LVL_FATAL, "Couldn't determine a firestore key from the instance_key, ending run.")
            sys.exit(1)

        db_size_key = firestore_size_key(db_size)

        resource_ref = db.collection("db_resources").document(firestore_db_key).collection("sizes").document(db_size_key).collection("resources").document(doc_name(db_type, db_size))

        traffic_base_ref = db.collection("events").document("next2020").collection(TMP_CACHED_DOC_NAME).document(key).collection("patterns")

        for pattern in traffic_patterns:
            traffic_ref = traffic_base_ref.document(pattern).collection("transactions")
            traffic = traffic_ref.limit(1).stream()

            if len(list(traffic)):
#                writeLog(LOG_LVL_INFO, f"Already fetched pattern for {key} with pattern {pattern}")
                continue

            # Test if we've warmed up the instance or not as first runs
            # are always pretty ugly
            warmed_id = f"{db_type}-{db_size}"
            if not warmed_id in warmed_instances:
                warm_instance(key, warmed_id)
                wait_on_instance(resource_ref)


            run_result = run_data(pattern, key)
            writeLog(LOG_LVL_INFO, f"Ran {key}, {pattern}: {run_result}")
            if run_result != None:
                JOBS_FILE.write(run_result)
#                output = f"./transfer.sh {key} {pattern} {run_result}\n"
#                print(f"{output}\n")

            wait_on_instance(resource_ref)

    writeLog(LOG_LVL_INFO, "**** Finished data run ****")
    JOBS_FILE.close()


# Run for every combination of instance, size and pattern
do_data_run()

# Wait two minutes to ensure that the last data gets finally written.
# Sometimes there's a delay in Dataflow -> Firestore
time.sleep(120)

success = validate_and_copy_data()
if not success:
    print("There were one or more problems validating the data set, check logs before proceeding.")
    sys.exit(1)


