import os
import re
import subprocess

import firebase_admin
from firebase_admin import credentials, firestore

db = None

def add_arguments(parser_obj):
    parser_obj.add_argument("-v", "--version", required=True,help="This is the version specified for the load-gen-script container. Should be in the format 'vx.x.x', e.g. v0.0.2")
    parser_obj.add_argument("-r", "--region", help="Specify a region to create your Cloud Run instances")

def auth_gcloud():
    try:
        subprocess.run(["gcloud auth login", "--brief"], shell=True, check=True, capture_output=True)
    except:
        print("Couldn't authenticate gcloud")
        return False
    return True


def enable_services():
    services_process = subprocess.run(["gcloud services enable run.googleapis.com iam.googleapis.com sqladmin.googleapis.com container.googleapis.com firestore.googleapis.com pubsub.googleapis.com dataflow.googleapis.com containerregistry.googleapis.com spanner.googleapis.com sql-component.googleapis.com"], shell=True, capture_output=True, text=True)
    if services_process.returncode != 0:
        print("There was a problem enabling GCP services")
        print(services_process.stderr)
        return False
    return True

# NOTE: Several of our APIs need a service account 
# with appropriate permissions to run. To this end,
# you'll need a service account created with the following
# permissions:
# - Cloud SQL Client
# - Dataflow Admin
# - Firebase Develop Admin
# - Pub/Sub Editor
# - Kubernetes Engine Admin
def create_service_account(project_id):
    sa_name = "break-service-test"
    sa_process = subprocess.run(["gcloud iam service-accounts create {} --display-name \"{} service account\"".format(sa_name, sa_name)], shell=True, capture_output=True, text=True)
    if sa_process.returncode != 0:
        err = sa_process.stderr
        x = re.search("is the subject of a conflict", err)
        if not x:
            print("There was a problem creating the service account")
            print (err)
            return None

    full_name = "{}@{}.iam.gserviceaccount.com".format(sa_name, project_id)
        # We need firebase and datastore at higher levels because
        # Firestore doesn't have gcloud support, so we need to do
        # everything via APIs rather than gcloud for it
    sa_roles = [ "cloudsql.client", "firebase.admin", "datastore.owner", "spanner.databaseUser" ]
    for role in sa_roles:
        proc = subprocess.run(["gcloud projects add-iam-policy-binding {} --member serviceAccount:{} --role roles/{}".format(project_id, full_name, role)], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr
            x = re.search("is the subject of a conflict", err)
            if not x:
                print("There was a problem assigning roles to the service account")
                print (err)
                return None

    json_path = "{}/breaking-service-account.json".format(os.path.dirname(os.getcwd()))

    if not os.path.exists(json_path):
        print("Downloading service account bearer token")
        proc = subprocess.run(["gcloud iam service-accounts keys create {} --iam-account {}".format(json_path, full_name)], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            print("Wasn't able to download the service account bearer token")
            return None
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    return full_name

def fetch_default_region(arg_region):
        # If they specified a region, use that
    if arg_region != None:
        return arg_region

        # Try to grab the default Cloud Run region
    default_region_process = subprocess.run(["gcloud config get-value run/region"], shell=True, capture_output=True, text=True)
    if default_region_process.stdout != "":
        return default_region_process.stdout.rstrip()

        # Next we'll go for a default Compute region
    default_region_process = subprocess.run(["gcloud config get-value compute/region"], shell=True, capture_output=True, text=True)
    if default_region_process.stdout != "":
        return default_region_process.stdout.rstrip()

        # Finally we'll go for an environment variable they can set if they don't want to use a default region
    default_region = os.environ.get("DEMO_REGION")
    if default_region == None:
        print("\nWasn't able to determine a region to start our Cloud Run services.\nPlease either set a region using 'gcloud config set run/region <region>'\nor set an environment variable 'DEMO_REGION' with the name of the region.\nEnsure that the region is a valid one. Regions can be found by running\n'gcloud compute regions list'.\n")
    return default_region

def fetch_project_id():
    project_id_process = subprocess.run(["gcloud config get-value project"], shell=True, capture_output=True, text=True)
    if project_id_process.stdout != "":
        return project_id_process.stdout.rstrip()

    print("Something went wrong with authorization with gcloud. Please try again and be sure to authorize when it pops up in your browser.")
    return None

def create_sql_instances(default_region, vm_cpus, vm_ram, instance_names):
    i = 0
    for name in instance_names:
        cpu = vm_cpus[i]
        ram = vm_ram[i]
        # Need to special case our initial instance because we
        # want it to be weak intentionally, and custom machines
        # aren't weak enough...so using the --tier flag to create
        # a micro instance (.6 GiB RAM and 1 vCPU) as our first instace
        db_create_process = None
        if cpu == "1":
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --tier={} --network=default".format(name, default_region, "db-f1-micro")], shell=True, capture_output=True, text=True)
        else:
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --cpu={} --memory={} --network=default".format(name, default_region, cpu, ram)], shell=True, capture_output=True, text=True)

        if db_create_process.returncode != 0:
            err = db_create_process.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print("There was a problem creating instance: '{}'".format(name))
                print (err)
                return False

        print("Instance '{}' created with '{}' CPU(s) and '{}' RAM.".format(name, cpu, ram))
        i = i + 1
    return True


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

# This is pretty hacky. There's likely a cleaner way
# to do this, and we'll have to rejigger this when we
# add more than one database resource per type/size
def set_sql_db_resources(names):
    global db

    cloud_sql_base_collection = db.collection(u'db_resources').document(u'cloud-sql').collection(u'sizes')

    cloud_sql_sm_collection = cloud_sql_base_collection.document(u'1x').collection(u'resources')

    cloud_sql_med_collection = cloud_sql_base_collection.document(u'2x').collection(u'resources')

    cloud_sql_lrg_collection = cloud_sql_base_collection.document(u'4x').collection(u'resources')

    i = 0
    for name in names:
        db_details_process = subprocess.run(["gcloud sql instances list | grep {}".format(name)], shell=True, capture_output=True, text=True)

        if db_details_process.returncode != 0:
            print("Something went wrong getting our Cloud SQL instance details, try again later.")
            return False
        out = db_details_process.stdout.split()[5]
        cloud_sql_doc = None
        if i == 0:
            cloud_sql_doc = cloud_sql_sm_collection.document(out)
        elif i == 1:
            cloud_sql_doc = cloud_sql_med_collection.document(out)
        elif i == 2:
            cloud_sql_doc = cloud_sql_lrg_collection.document(out)

        cloud_sql_doc.set({ u'expiry': 0 })
        print("Added {} to Firestore".format(name))
        i = i + 1

    return True