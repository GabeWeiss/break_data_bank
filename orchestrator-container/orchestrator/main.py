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

from quart import Quart, websocket, jsonify, request

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

async def do_run(db_type,
                 db_size,
                 read_pattern,
                 write_pattern,
                 read_intensity,
                 write_intensity):
    print ("\n\nwat\n\n")

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

# sets job data on firestore and returns a job_id
async def set_firestore(db_type,
                        db_size,
                        read_pattern,
                        write_pattern,
                        read_intensity,
                        write_intensity):
    jobs_collection = db.collection(u'events').document(u'next2020').collection('jobs')
    #jobs_collection.add()

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
        read_pattern = form['read_pattern']
        write_pattern = form['write_pattern']
    except: 
        return "\nMissing a required parameter, please ensure you have everything in your POST method.\n", 400

    jobs_id = await set_firestore(read_pattern, write_pattern, )


    #await do_run(1,2,3,4,5,6)
    return '\n{}\n\n'.format(write_pattern), 200

@app.route('/run', methods=['POST'])
async def run():
    return 'run'
"""
@app.websocket('/ws')
async def ws():
    while True:
        await websocket.send('hello')
"""
app.run()
