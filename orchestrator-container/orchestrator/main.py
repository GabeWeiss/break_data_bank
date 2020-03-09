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

import sys
import time

import asyncio
from quart import Quart, websocket, jsonify, request
import requests

import firebase_admin
from firebase_admin import credentials, db, firestore

### STATIC VALUES/ENUMS (be sure to change load_gen and resource allocate scripts also if these change)
# TODO gweiss: Move this into a shared space for all scripts
TRAFFIC_LOW    = 1
TRAFFIC_HIGH   = 2
TRAFFIC_SPIKEY = 3

CLOUD_SQL         = 1
CLOUD_SQL_REPLICA = 2
CLOUD_SPANNER     = 3

app = Quart(__name__)

# CHANGE THIS FOR FINAL PROD
gDuration = 3 # represents the duration we're reserving an instance
db_lease_url = "http://localhost:5003/lease"

cred = credentials.Certificate('../break_service_account.json')
firebase_admin.initialize_app(cred, {
    'databaseURL' : 'https://my-db.firebaseio.com'
})
db = firestore.client()

@app.route('/')
async def index():
    return """<html><body>
        Supported paths:
        <ul>
            <li>test</li>
            <li>fail</li>
            <li>run</li>
        </ul>
    </body></html>"""

async def fetch_resource_id(db_type, db_size, duration):
    parameters = {'database_type':db_type,'database_size':db_size,'duration':duration}
    r = requests.post(url = db_lease_url, json = parameters)
    print(r.text)
    return "0.0.0.0"

async def do_run(db_type, db_size,
                 read_pattern, read_intensity,
                 write_pattern, write_intensity):
        # Fetch a resource
    resource_id = await fetch_resource_id(db_type, db_size, gDuration)
    print (resource_id)
        # Start load gen
        # TODO: start load gen

@app.route('/test', methods=['POST', 'GET'])
async def test():
    form = await request.form
    tag = "foo"
    try:
        val = form[tag]
    except:
        return "\nUnknown tag: {}\n\n".format(tag), 400
    response = {
        "resource_id": val,
        "expiration": time.time()
    }
    return '{}\n'.format(response), 200

# sets job data on firestore and returns a job_id back to the caller
async def set_firestore(db_type, db_size,
                        read_pattern, read_intensity,
                        write_pattern, write_intensity):
    new_jobs_document = db.collection(u'events').document(u'next2020').collection('jobs').document()
    new_jobs_document.set({
        u'db_type': db_type,
        u'db_size': db_size,
        u'read_pattern': read_pattern,
        u'read_intensity': read_intensity,
        u'write_pattern': write_pattern,
        u'write_intensity': write_intensity
    })
    return new_jobs_document.id

# This is the entry point for the first run where we're
# intentionally failing the Cloud SQL. Assumption currently
# is that the front-end will send the same format, but that
# we will only care about some of the parameters for this one
# vs. the "real" attempt
@app.route('/fail', methods=['POST'])
async def fail():
    # validate we have the data we need from the caller
    form = await request.form
    try:
        # Note, for fail run, these are the only two we care about
        read_pattern = form["read_pattern"]
        write_pattern = form["write_pattern"]
    except: 
        return "\nMissing required parameters ('read_pattern', 'write_pattern'), please ensure you have everything in your POST method.\n", 400

    jobs_id = await set_firestore(CLOUD_SQL, 1, int(read_pattern), int(write_pattern), 3, 3)

        # Using the event loop so we can initiate the load gen
        # asynchronously while returning our id back to the 
        # front-end so they can subscribe. MIGHT even need a small
        # sleep here to give the front-end time to subscribe
        # appropriately
    loop = asyncio.get_event_loop()
    loop.create_task(do_run(CLOUD_SQL,1,read_pattern,3,write_pattern,3))
    return '{{ jobs_id: "{}" }}'.format(jobs_id), 200

@app.route('/run', methods=['POST'])
async def run():
    return 'run', 200
"""
@app.websocket('/ws')
async def ws():
    while True:
        await websocket.send('hello')
"""
app.run(host="localhost",port="5001")
