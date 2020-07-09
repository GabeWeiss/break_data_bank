import datetime
import numpy as np
from scipy import stats
import sys
import time

from google.cloud import firestore

from utils_breaking import *

db = firestore.Client()

# Validation variables
TRANSACTION_COUNT_MINIMUM = 40
# In milliseconds
MINIMUM_TIME_LENGTH = 20000
# Expressed as a percent over the average
LATENCY_Z_SCORE_THRESHOLD = 8

def validate_and_copy_data(jobs_filename, cached_location):
    writeLog(LOG_LVL_INFO, "**** Beginning transactions validation ****")
    success = True

    JOBS_FILE = open(jobs_filename, "r")
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
            key = db_key(db_type, db_size, intensity)
            pattern = f"{read_pattern}-{write_pattern}"
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
            cache_ref = db.collection("events").document("next2020").collection(TMP_CACHED_DOC_NAME).document(key).collection("patterns").document(pattern).collection("transactions")
            writeLog(LOG_LVL_INFO, f" Moving transactions to staging cache")
            for t in transactions:
                t_dict = t.to_dict()
                cache_ref.add(t_dict)


    JOBS_FILE.close()
    return success

