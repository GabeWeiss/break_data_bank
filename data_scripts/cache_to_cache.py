import sys

from google.cloud import firestore

db = firestore.Client()

SEED = 0

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

for instance in instance_keys:
    print(f"Running for instance: {instance}")
    db_info = instance.split("-")
    db_type = db_info[0]
    if db_type == -1:
        print("Couldn't get db_type")
        sys.exit(1)
    try:
        db_size = int(db_info[1])
    except:
        print("Couldn't get the db information properly")
        sys.exit(1)

    for traffic in traffic_keys:
        source_collection_ref = db.collection("events").document("next2020").collection("cached_staged").document(instance).collection("patterns").document(traffic).collection("transactions")

        target_collection_ref = db.collection("events").document("next2020").collection("cached").document(instance).collection("patterns").document(traffic).collection("transactions")

        transactions = source_collection_ref.stream()
        for t in transactions:
            doc_dict = t.to_dict()
            doc_dict['seed'] = SEED
            target_collection_ref.add(doc_dict)
