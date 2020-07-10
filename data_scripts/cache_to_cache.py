import argparse
import sys

from google.cloud import firestore

parser = argparse.ArgumentParser()
# TODO: Add ability to set seed and transfer directly to production
#parser.add_argument("-s", "--seed")
parser.add_argument("--seed", required=True)
args = parser.parse_args()

db = firestore.Client()

try:
    SEED = int(args.seed)
except:
    print("You need to specify an integer for the seed.")
    sys.exit(1)

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

traffic_keys = [
    "1-1", "1-2", "1-3", "2-1", "2-2", "2-3", "3-1", "3-2", "3-3"
]

def transfer():
    for instance in instance_keys:
        print(f"Running for instance: {instance}")

        for traffic in traffic_keys:
            source_collection_ref = db.collection("events").document("next2020").collection("cached_staged").document(instance).collection("patterns").document(traffic).collection("transactions")

            target_collection_ref = db.collection("events").document("next2020").collection("cached").document(instance).collection("patterns").document(traffic).collection("transactions")

            transactions = source_collection_ref.stream()
            for t in transactions:
                doc_dict = t.to_dict()
                doc_dict['seed'] = SEED
                target_collection_ref.add(doc_dict)

def will_overwrite():
    for instance in instance_keys:
        target_collection_ref = db.collection("events").document("next2020").collection("cached").document(instance).collection("patterns").document("1-1").collection("transactions")
        targets = target_collection_ref.limit(1).where('seed', '==', SEED).stream()
        if len(list(targets)) > 0:
            return True
    return False

if will_overwrite():
    print("This would overwrite existing data in the database. Please delete any transactions in the way before continuing.")
    sys.exit(1)

transfer()
