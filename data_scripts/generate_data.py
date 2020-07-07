#!/usr/bin/python

import argparse
import firebase_admin
from firebase_admin import credentials, firestore
import random
import time

TRAFFIC_LOW    = 1
TRAFFIC_HIGH   = 2
TRAFFIC_SPIKEY = 3

SQL_READ_LATENCY = 0.0008
SQL_WRITE_LATENCY = 0.001
SQLREP_READ_LATENCY = 0.0008
SQLREP_WRITE_LATENCY = 0.006
SPANNER_READ_LATENCY = 0.002
SPANNER_WRITE_LATENCY = 0.01

def starting_read_latency(db_type):
    if db_type == "sql":
        return SQL_READ_LATENCY
    if db_type == "sqlrep":
        return SQLREP_READ_LATENCY
    if db_type == "spanner":
        return SPANNER_READ_LATENCY

def starting_write_latency(db_type):
    if db_type == "sql":
        return SQL_WRITE_LATENCY
    if db_type == "sqlrep":
        return SQLREP_WRITE_LATENCY
    if db_type == "spanner":
        return SPANNER_WRITE_LATENCY

db = None
def initialize_firestore():
    global db
    # Passing "None" here means use the application default credentials
    try:
        firebase_admin.initialize_app(None)
        db = firestore.client()
    except:
        print("There was a problem initializing Firestore")
        return False
    return True

def decline(db_type, progress, action, intensity_n):
    sql_decline_1_r = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,2.0]
    sql_decline_2_r = [0.0,0.0,0.0,0.0,0.0,1.0,1.0,2.0,3.0,4.0]
    sql_decline_3_r = [0.0,0.0,1.0,1.0,2.0,3.0,4.0,4.0,5.0,7.0]
    sql_decline_1_w = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,2.0]
    sql_decline_2_w = [0.0,0.0,0.0,0.0,1.0,1.0,2.0,3.0,4.0,6.0]
    sql_decline_3_w = [0.0,1.0,2.0,2.0,3.0,4.0,5.0,7.0,9.0,10.0]

    sql_failure_1 = [0,0,0,0,0,0,0,0,0,5]
    sql_failure_2 = [0,0,0,0,0,0,0,5,5,10]
    sql_failure_3 = [0,0,0,0,5,5,10,20,50,100]

    sqlrep_decline_1_r = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,2.0]
    sqlrep_decline_2_r = [0.0,0.0,0.0,0.0,0.0,1.0,1.0,2.0,3.0,4.0]
    sqlrep_decline_3_r = [0.0,0.0,1.0,1.0,2.0,3.0,4.0,4.0,5.0,7.0]
    sqlrep_decline_1_w = [0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,2.0,2.0]
    sqlrep_decline_2_w = [0.0,0.0,1.0,1.0,2.0,2.0,3.0,4.0,6.0,7.0]
    sqlrep_decline_3_w = [1.0,2.0,3.0,4.0,5.0,5.0,7.0,9.0,10.0,10.0]

    sqlrep_failure_1 = [0,0,0,0,0,0,0,5,5,10]
    sqlrep_failure_2 = [0,0,0,0,0,5,10,15,25,35]
    sqlrep_failure_3 = [0,0,0,5,10,15,25,35,75,100]

    spanner_decline_1_r = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,2.0]
    spanner_decline_2_r = [0.0,0.0,0.0,0.0,0.0,1.0,1.0,2.0,3.0,4.0]
    spanner_decline_3_r = [0.0,0.0,1.0,1.0,2.0,3.0,4.0,4.0,5.0,7.0]
    spanner_decline_1_w = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,2.0]
    spanner_decline_2_w = [0.0,0.0,0.0,0.0,1.0,1.0,2.0,3.0,4.0,6.0]
    spanner_decline_3_w = [0.0,1.0,2.0,2.0,3.0,4.0,5.0,7.0,9.0,10.0]

    spanner_failure_1 = [0,0,0,0,0,0,0,0,0,0]
    spanner_failure_2 = [0,0,0,0,0,0,0,1,3,2]
    spanner_failure_3 = [0,0,4,2,2,5,3,4,5,4]

    decline_multiplier = 0
    failure_percent = 0
    decline = 0


    #print("{}, {}, {}, {}".format(db_type, intensity, action, progress))
    if db_type == "sql":
        if intensity == 1:
            failure_percent = sql_failure_1[progress]
            if action == "read":
                decline_multiplier = sql_decline_1_r[progress]
                if decline_multiplier == 0:
                    decline = SQL_READ_LATENCY
                else:
                    decline = decline_multiplier * SQL_READ_LATENCY
            elif action == "write":
                decline_multiplier = sql_decline_1_w[progress]
                if decline_multiplier == 0:
                    decline = SQL_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQL_WRITE_LATENCY
        elif intensity == 2:
            failure_percent = sql_failure_2[progress]
            if action == "read":
                decline_multiplier = sql_decline_2_r[progress]
                if decline_multiplier == 0:
                    decline = SQL_READ_LATENCY
                else:
                    decline = decline_multiplier * SQL_READ_LATENCY
            elif action == "write":
                decline_multiplier = sql_decline_2_w[progress]
                if decline_multiplier == 0:
                    decline = SQL_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQL_WRITE_LATENCY
        elif intensity == 3:
            failure_percent = sql_failure_3[progress]
            if action == "read":
                decline_multiplier = sql_decline_3_r[progress]
                if decline_multiplier == 0:
                    decline = SQL_READ_LATENCY
                else:
                    decline = decline_multiplier * SQL_READ_LATENCY
            elif action == "write":
                decline_multiplier = sql_decline_3_w[progress]
                if decline_multiplier == 0:
                    decline = SQL_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQL_WRITE_LATENCY
    elif db_type == "sqlrep":
        if intensity == 1:
            failure_percent = sqlrep_failure_1[progress]
            if action == "read":
                decline_multiplier = sqlrep_decline_1_r[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_READ_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_READ_LATENCY
            elif action == "write":
                decline_multiplier = sqlrep_decline_1_w[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_WRITE_LATENCY
        elif intensity == 2:
            failure_percent = sqlrep_failure_2[progress]
            if action == "read":
                decline_multiplier = sqlrep_decline_2_r[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_READ_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_READ_LATENCY
            elif action == "write":
                decline_multiplier = sqlrep_decline_2_w[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_WRITE_LATENCY
        elif intensity == 3:
            failure_percent = sqlrep_failure_3[progress]
            if action == "read":
                decline_multiplier = sqlrep_decline_3_r[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_READ_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_READ_LATENCY
            elif action == "write":
                decline_multiplier = sqlrep_decline_3_w[progress]
                if decline_multiplier == 0:
                    decline = SQLREP_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SQLREP_WRITE_LATENCY
    elif db_type == "spanner":
        if intensity == 1:
            failure_percent = spanner_failure_1[progress]
            if action == "read":
                decline_multiplier = spanner_decline_1_r[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_READ_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_READ_LATENCY
            elif action == "write":
                decline_multiplier = spanner_decline_1_w[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_WRITE_LATENCY
        elif intensity == 2:
            failure_percent = spanner_failure_2[progress]
            if action == "read":
                decline_multiplier = spanner_decline_2_r[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_READ_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_READ_LATENCY
            elif action == "write":
                decline_multiplier = spanner_decline_2_w[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_WRITE_LATENCY
        elif intensity == 3:
            failure_percent = spanner_failure_3[progress]
            if action == "read":
                decline_multiplier = spanner_decline_3_r[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_READ_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_READ_LATENCY
            elif action == "write":
                decline_multiplier = spanner_decline_3_w[progress]
                if decline_multiplier == 0:
                    decline = SPANNER_WRITE_LATENCY
                else:
                    decline = decline_multiplier * SPANNER_WRITE_LATENCY

    random_fail_jitter = random.randint(-3, 3)
    random_latency_jitter = random.uniform(0.0, 0.2)
    if decline * random_latency_jitter > 0:
        decline = decline * random_latency_jitter
    if failure_percent * random_fail_jitter > 0:
        failure_percent * random_fail_jitter
    return decline, failure_percent

def build_job_string(type_string, size, intensity):
    return "{}-{}-{}".format(type_string, size, intensity)

def build_pattern_string(read, write):
    return "{}-{}".format(read, write)

def collection_ref(type_string, size, intensity, read_p, write_p):
    return db.collection("events").document("next2020").collection("cached").document(build_job_string(type_string, size, intensity)).collection("patterns").document(build_pattern_string(read_p, write_p)).collection("transactions")

def write_transaction(docref, data):
    docref.add(data)

def build_transaction(timestamp, latency, failure, action, seed):
    return {
        u'timestamp': timestamp,
        u'average_latency': latency,
        u'failure_percent': failure,
        u'query_action': action,
        u'seed': seed
    }

    return None

random.seed()

initialize_firestore()

timestamp = time.time()
db_types = ["sql", "sqlrep", "spanner"]

for db_type in db_types:
    for size in range(1,4):
        for read_pattern in range(1,4):
            for write_pattern in range(1,4):
                for intensity in range(1,4):
                    ref = collection_ref(db_type, size, intensity, read_pattern, write_pattern)
                    for prog in range(10):
                        for seed in range(5):
                            c_timestamp = timestamp + prog

                            read,fail = decline(db_type, prog, "read", intensity)
                            write,fail = decline(db_type, prog, "write", intensity)

                            #print("read: {}\nwrite: {}\nfail: {}\n\n".format(read, write, fail))

                            write_transaction(ref, build_transaction(c_timestamp, read, fail, "read", seed))
                            write_transaction(ref, build_transaction(c_timestamp, write, fail, "write", seed))
