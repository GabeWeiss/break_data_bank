#!/usr/bin/python

import argparse
from google.oauth2 import service_account
from google.cloud import firestore
import re

parser = argparse.ArgumentParser(description='Transfer data between two Firestore instances')
parser.add_argument("src_db", help="Source Firestore project")
parser.add_argument("tgt_db", help="Target Firestore project")
parser.add_argument("src_account", help="Source service account path")
parser.add_argument("tgt_account", help="Target service account path")
parser.add_argument("src_uri", help="Source data uri")
parser.add_argument("tgt_uri", help="Target data uri")

args = parser.parse_args()

# Instantiate our two Firestore instances based on the passed in service accounts
# Need to do this way because we have two different references, potentially in
# two different projects
src_firestore = firestore.Client(args.src_db,
                                 service_account.Credentials.from_service_account_file(args.src_account))
tgt_firestore = firestore.Client(args.tgt_db,
                                 service_account.Credentials.from_service_account_file(args.tgt_account))

src_uri = args.src_uri
# Sanitize our uri path (the one from Firestore gives us a leading '/') for splitting
src_uri = re.sub(r'^/|/$', '', src_uri)

# Have to do this rigamarole because there's no way to get a basic reference as
# far as I can tell, have to alternate document and collection references
src_folders = src_uri.split("/")
src_ref = None
folder_type = 0
for src in src_folders:
    if src_ref == None:
        folder_type = 1
        src_ref = src_firestore.collection(src)
        continue
    if folder_type == 0:
        src_ref = src_ref.collection(src)
        folder_type = 1
    else:
        src_ref = src_ref.document(src)
        folder_type = 0

docs = src_ref.stream()

# Time to fetch our target reference to write to
tgt_uri = args.tgt_uri
# Sanitize our uri path (the one from Firestore gives us a leading '/') for splitting
tgt_uri = re.sub(r'^/|/$', '', tgt_uri)

# Have to do this rigamarole because there's no way to get a basic reference as
# far as I can tell, have to alternate document and collection references
tgt_folders = tgt_uri.split("/")
tgt_ref = None
folder_type = 0
for tgt in tgt_folders:
    if tgt_ref == None:
        folder_type = 1
        tgt_ref = tgt_firestore.collection(tgt)
        continue
    if folder_type == 0:
        tgt_ref = tgt_ref.collection(tgt)
        folder_type = 1
    else:
        tgt_ref = tgt_ref.document(tgt)
        folder_type = 0

# And last, move our source documents into target collection
for doc in docs:
    tgt_ref.add(doc.to_dict())
