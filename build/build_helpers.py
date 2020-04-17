#!/usr/bin/env python

import argparse
import os
import re
import requests
from shutil import copyfile
import subprocess
import time
import webbrowser

import firebase_admin
from firebase_admin import credentials, firestore

# These values appear in several of the service scripts
# If they change here, they must be changed everywhere
CLOUD_SQL = 1
CLOUD_SQL_READ_REPLICA = 2
CLOUD_SPANNER = 3

db = None

def add_arguments(parser_obj):
    parser_obj.add_argument("-v", "--version", required=True,help="This is the version specified for the load-gen-script container. Should be in the format 'vx.x.x', e.g. v0.0.2")
    parser_obj.add_argument("-r", "--region", help="Specify a region to create your Cloud Run instances")
    parser_obj.add_argument("-p", "--pubsub", help="Specifies a pub/sub topic, defaults to 'breaking-test'", default="breaking-demo-topic")

def verify_prerequisites():
    try:
        subprocess.run(["docker --version"], shell=True, check=True, capture_output=True)
        subprocess.run(["kubectl version"], shell=True, check=True, capture_output=True)
        subprocess.run(["mvn --version"], shell=True, check=True, capture_output=True)
        subprocess.run(["gcloud --version"], shell=True, check=True, capture_output=True)
        subprocess.run(["gsutil --version"], shell=True, check=True, capture_output=True)
        subprocess.run(["firebase --version"], shell=True, check=True, capture_output=True)
    except:
        print("\n\nYou're missing one of the prerequisites to run this build script. You must have Docker, kubectl, Maven, firebase CLI, gsutil and gcloud installed.\n\n")
        return False

    # They need the JDK in order to do the Dataflow piece
    if os.environ.get('JAVA_HOME') == None:
        print("Looks like you haven't installed the Java Development Kit. You need it to be able to build and deploy the Dataflow piece to this demo. Go here to install it:\n http://www.oracle.com/technetwork/java/javase/downloads/index.html\nDon't forget to also set the JAVA_HOME variable to point to the install directory.\n\n")
        return False
    return True

def auth_gcloud():
    try:
        subprocess.run(["gcloud auth login", "--brief"], shell=True, check=True, capture_output=True)
    except:
        print("Couldn't authenticate gcloud")
        return False
    return True

def auth_docker():
    try:
        subprocess.run(["gcloud auth configure-docker"], shell=True, check=True)
    except:
        print("There was a problem authorizing gcloud with docker\n")
        return False
    return True

def enable_services():
    services_process = subprocess.run(["gcloud services enable run.googleapis.com iam.googleapis.com sqladmin.googleapis.com container.googleapis.com firestore.googleapis.com pubsub.googleapis.com dataflow.googleapis.com containerregistry.googleapis.com spanner.googleapis.com sql-component.googleapis.com storage-component.googleapis.com servicenetworking.googleapis.com"], shell=True, capture_output=True, text=True)
    if services_process.returncode != 0:
        print("There was a problem enabling GCP services")
        print(services_process.stderr)
        return False
    return True

# NOTE: Several of our APIs need a service account 
# with appropriate permissions to run. To this end,
# we'll need a service account created with the following
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

    # We need a separate VPC because of allocating IP addresses for GKE
    # Since our load gen is going to consume a lot of IPs, we need to
    # allocate a broad range. Rather than risk running out of IPs, which
    # results in failing to create the GKE cluster, we'll just isolate
    # in our own VPC
def create_vpc():
    vpc_name = "breaking-vpc"
    proc = subprocess.run(["gcloud compute networks create {}".format(vpc_name)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("already exists", err)
        if not x:
            print("There was a problem creating our internal network")
            print(proc.stderr)
            return None
        else:
            print("WARNING: The VPC was already created. Beware the GKE cluster may fail to create later\n")
    allocation_name = "breaking"
    proc = subprocess.run(["gcloud compute addresses create {} --global --purpose=VPC_PEERING --prefix-length=24 --network={}".format(allocation_name, vpc_name)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("already exists", err)
        if not x:
            print("Wasn't able to allocate private IPs for your VPC network")
            print(err)
            return None
    else: # Only run the vpc-peerings if it's a new situation
        proc = subprocess.run(["gcloud services vpc-peerings update --service=servicenetworking.googleapis.com  --network={}     --project=gweiss-simple-path --ranges={} --force".format(vpc_name, allocation_name)], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr
            x = re.search("already exists", err)
            if not x:
                print("Wasn't able to connect private services")
                print(err)
                return None

    return vpc_name

def fetch_sql_region(arg_region, envvar):
        # If they specified a region, use that
    if arg_region != None:
        os.environ[envvar] = arg_region
        return arg_region

        # Try to grab the default Cloud Run region
    default_region_process = subprocess.run(["gcloud config get-value run/region"], shell=True, capture_output=True, text=True)
    if default_region_process.stdout != "":
        region = default_region_process.stdout.rstrip()
        os.environ[envvar] = region
        return region

        # Next we'll go for a default Compute region
    default_region_process = subprocess.run(["gcloud config get-value compute/region"], shell=True, capture_output=True, text=True)
    if default_region_process.stdout != "":
        region = default_region_process.stdout.rstrip()
        os.environ[envvar] = region
        return region

        # Finally we'll go for an environment variable they can set if they don't want to use a default region
    default_region = os.environ.get(envvar)
    if default_region == None:
        print("\nWasn't able to determine a region to start our Cloud Run services.\nPlease either set a region using 'gcloud config set run/region <region>'\nor set an environment variable '{}' with the name of the region.\nEnsure that the region is a valid one. Regions can be found by running\n'gcloud compute regions list'.\n".format(envvar))
    return default_region

def extrapolate_spanner_region(sql_region):
    x = re.search("^asia\-", sql_region)
    if x:
        return "nam-eur-asia1"
    x = re.search("^australia\-", sql_region)
    if x:
        return "nam-eur-asia1"
    x = re.search("^europe\-", sql_region)
    if x:
        return "eur3"
    x = re.search("^northamerica\-", sql_region)
    if x:
        return "nam6"
    x = re.search("^southamerica\-", sql_region)
    if x:
        return "nam6"
    x = re.search("^us\-", sql_region)
    if x:
        return "nam6"

    print("Couldn't figure out how to extrapolate from the region: '{}'".format(sql_region))
    return None

def extrapolate_firestore_region(sql_region):
    x = re.search("^asia\-", sql_region)
    if x:
        return "asia-northeast1"

    x = re.search("^australia\-", sql_region)
    if x:
        return "australia-southeast1"

    x = re.search("^europe\-", sql_region)
    if x:
        return "europe-west"

    x = re.search("^northamerica\-", sql_region)
    if x:
        return "us-central"

    x = re.search("^southamerica\-", sql_region)
    if x:
        return "us-central"

    x = re.search("^us\-central", sql_region)
    if x:
        return "us-central"

    x = re.search("^us\-west", sql_region)
    if x:
        return "us-west2"

    x = re.search("^us\-east", sql_region)
    if x:
        return "us-east1"

    print("Couldn't figure out how to extrapolate from the region: '{}'".format(sql_region))
    return None

def fetch_project_id(envvar):
    project_id_process = subprocess.run(["gcloud config get-value project"], shell=True, capture_output=True, text=True)
    if project_id_process.stdout != "":
        proj = project_id_process.stdout.rstrip()
        os.environ[envvar] = proj
        return proj

    print("Something went wrong with authorization with gcloud. Please try again and be sure to authorize when it pops up in your browser.")
    return None

# There's a default value set in argparse, so this SHOULDN'T ever
# have a None value passed in, but just in case, put in the test
def fetch_pubsub_topic(pubsub, envvar):
    if pubsub != None:
        os.environ[envvar] = pubsub
        return pubsub
    return None

def create_pubsub_topic(pubsub_topic):
    proc = subprocess.run(["gcloud pubsub topics create {}".format(pubsub_topic)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("Resource already exists", err)
        if not x:
            print("There was a problem creating the Pub/Sub topic")
            print(err)
            return False
    return True

def create_sql_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc):
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
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --tier={} --network={}".format(name, sql_region, "db-f1-micro", vpc)], shell=True, capture_output=True, text=True)
        else:
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --cpu={} --memory={} --network={}".format(name, sql_region, cpu, ram, vpc)], shell=True, capture_output=True, text=True)

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

        print(" Instance '{}' created with '{}' CPU(s) and '{}' RAM.".format(name, cpu, ram))
        i = i + 1

    print("")
    return True

def create_sql_replica_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc):
    i = 0
    for short_name in instance_names:
        name = "{}-r".format(short_name)
        cpu = vm_cpus[i]
        ram = vm_ram[i]

        # Need to special case our initial instance because we
        # want it to be weak intentionally, and custom machines
        # aren't weak enough...so using the --tier flag to create
        # a micro instance (.6 GiB RAM and 1 vCPU) as our first instace
        db_create_process = None
        if cpu == "1":
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --tier={} --network={}".format(name, sql_region, "db-f1-micro", vpc)], shell=True, capture_output=True, text=True)
        else:
            db_create_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --cpu={} --memory={} --network={}".format(name, sql_region, cpu, ram, vpc)], shell=True, capture_output=True, text=True)

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

            # NOTE: There's a workaround in here for b/145025740
            # I've removed all references to --tier, --ram and --cpu. This means,
            # currently, it should grab the tier of the master instance which
            # is fine. We SHOULD be able to specify different specs for the replica
            # than the master, but currently we can't
        replica_name = "{}-replica".format(name)
        replica_process = None
        if cpu == "1":
            replica_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --network={} --master-instance-name={}".format(replica_name, sql_region, vpc, name)], shell=True, capture_output=True, text=True)
        else:
            replica_process = subprocess.run(["gcloud beta sql instances create {} --database-version=POSTGRES_11 --region={} --no-backup --no-assign-ip --root-password=postgres --network={} --master-instance-name={}".format(replica_name, sql_region, vpc, name)], shell=True, capture_output=True, text=True)
        
        if replica_process.returncode != 0:
            err = replica_process.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print("There was a problem creating instance: '{}'".format(name))
                print (err)
                return False

        print(" Instance '{}' created with '{}' CPU(s) and '{}' RAM. With read replica: '{}'".format(name, cpu, ram, replica_name))
        i = i + 1

    print("")
    return True

def create_spanner_instances(instance_names, spanner_region, nodes, spanner_descriptions):
    i = 0
    for name in instance_names:
        node_count = nodes[i]
        description = spanner_descriptions[i]
        proc = subprocess.run(["gcloud spanner instances create {} --nodes={} --config={} --description=\"{}\"".format(name, node_count, spanner_region, description)], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print("There was a problem creating instance: '{}'".format(name))
                print (err)
                return False

        print(" Spanner instance '{}' created with {} nodes".format(name, node_count))
        i = i + 1

    print("")
    return True

def run_firestore_create(region):
    # Alpha has a create database in gcloud, we'll go ahead and dogfood this...
    proc = subprocess.run(["gcloud alpha firestore databases create --region={}".format(region)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        return proc.stderr, False

    return None

def initialize_firestore(project_id, region):
    create_err = run_firestore_create(region)
    if create_err != None:
        x = re.search("You must first create an Google App Engine app", create_err)
        if not x:
            print("   There was a problem creating the Firestore database.")
            print(proc.stderr)
            return False

        proc = subprocess.run(["gcloud app create --region={}".format(region)], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            print("   Wasn't able to create our default app engine application to enable Firestore database creation.")
            print(proc.stderr)
            return False
        
        # Try one more time now that we have our app engine created
        create_err = run_firestore_create(region)
        if create_err != None:
            print("   There was a problem creating the Firestore database.")
            print(proc.stderr)
            return False

    # Setup our indexes we'll need for querying later
    # This involves creating a tmp directory for firestore deploying purposes
    firestore_dir = './firestore_setup'
    try:
        os.makedirs(firestore_dir)
    except:
        print("  Couldn't create setup directory. This may cause Firestore to be improperly setup.")

    # This is hacky as heck, but we need to be sure that Firebase, is also added
    # to the project, not just Firestore, because otherwise we can't deploy
    # Firestore rules
    proc = subprocess.run(["firebase projects:addfirebase {}".format(project_id)], shell=True, text=True, capture_output=True, cwd=firestore_dir)
    if proc.returncode != 0:
        debug_proc = subprocess.run(["tail -n 20 firebase-debug.log"], shell=True, text=True, capture_output=True, cwd=firestore_dir)
        if debug_proc.returncode != 0:
            print("   Something went wrong trying to get the debug output from the firebase CLI error")
            print(debug_proc.stderr)
            return False
        out = debug_proc.stdout
        x = re.search("is already a Firebase project", out)
        if not x:
            print("   Wasn't able to add Firestore initialization to the environment")
            print(out)
            return False

    print("\nThis next section may ask you to pick names for certain resources. Go ahead and just hit enter and leave the defaults the way they are.\n")
    print("Press return to continue...")
    y = input()

    proc = subprocess.run(["firebase init firestore -P {}".format(project_id)], shell=True, text=True, cwd=firestore_dir)
    if proc.returncode != 0:
        print("   Wasn't able to initialize firestore for our project.")
        print(proc.stderr)
        return False

    try:
        copyfile('firestore.indexes', "{}/firestore.indexes.json".format(firestore_dir))
    except:
        print("   Wasn't able to copy our index files")
        return False

    proc = subprocess.run(["firebase deploy --only firestore:indexes"], shell=True, text=True, capture_output=True, cwd=firestore_dir)
    if proc.returncode != 0:
        print("   There was a problem creating our Firestore indexes.")
        print(proc.stderr)
        return False

    return False

    global db
    # Passing "None" here means use the application default credentials
    try:
        firebase_admin.initialize_app(None)
        db = firestore.client()
    except:
        print("There was a problem initializing Firestore")
        return False
    return True

def add_db(url, database_type, database_size, resource_id, connection_string, replica_ip):
    parameters = {'database_type':database_type,'database_size':database_size,'resource_id':resource_id, 'connection_string':connection_string, 'replica_ip':replica_ip }
    print(parameters)
    r = requests.post(url = url, json = parameters)
    print(r.text)
    return True

# Make use of the db-lease service to add our dbs to Firestore
def set_sql_db_resources(names, url):
    # Service needs:
    #   "database_type"
    #   "database_size"
    #   "resource_id"
    #   "connection_string"
    #   "replica_ip" <-- when DB is type CLOUD_SQL_READ_REPLICA
    INST_POS_RESOURCE_ID = 0
    INST_POS_IP = 5
    current_db_type = CLOUD_SQL

    for name in names:
        print("Processing name: {}".format(name))
        database_size = -1
        database_type = CLOUD_SQL # Starts here
        if re.search("sm", name):
            database_size = 1
        elif re.search("med", name):
            database_size = 2
        elif re.search("lrg", name):
            database_size = 3

        if database_size == -1:
            print("Couldn't figure out what size our database is ({}), so it won't be added to our Firestore metadata.".format(name))
            return False

        resource_id = None
        connection_string = None
        replica_ip = None

        # First we handle Cloud SQL
        info_proc = subprocess.run(["gcloud sql instances list | grep \"{} \"".format(name)], shell=True, capture_output=True, text=True)
        if info_proc.returncode != 0:
            print("There was a problem fetching the main sql instance info")
            print(info_proc.stderr)
            return False
        out = info_proc.stdout

        instance_data = out.split()
        try:
            resource_id = instance_data[INST_POS_RESOURCE_ID]
            connection_string = instance_data[INST_POS_IP]
        except:
            print("Looks like the format of the gcloud sql instances list output changed")
            return False

        if not add_db(url, database_type, database_size, resource_id,       connection_string, None):
            return False

        # Next we handle the replica
        master_proc = subprocess.run(["gcloud sql instances list | grep \"{}-r \"".format(name)], shell=True, capture_output=True, text=True)
        if master_proc.returncode != 0:
            print("There was a problem fetching the replica info")
            print(master_proc.stderr)
            return False
        master_out = master_proc.stdout

        replica_proc = subprocess.run(["gcloud sql instances list | grep \"{}-r-replica \"".format(name)], shell=True, capture_output=True, text=True)
        if replica_proc.returncode != 0:
            print("There was a problem fetching the replica info")
            print(replica_proc.stderr)
            return False
        replica_out = replica_proc.stdout

        master_data = master_out.split()
        replica_data = replica_out.split()

        database_type = CLOUD_SQL_READ_REPLICA
        try:
            resource_id = master_data[INST_POS_RESOURCE_ID]
            connection_string = master_data[INST_POS_IP]
            replica_ip = replica_data[INST_POS_IP]
        except:
            print("Wasn't able to get our replica data. The data format might have changed")
            return False

        if not add_db(url, database_type, database_size, resource_id,       connection_string, replica_ip):
            return False

    return True

def set_spanner_db_resources(names, url):
    global db

    cloud_spanner_base_collection = db.collection(u'db_resources').document(u'cloud-spanner').collection(u'sizes')
    sizes = [ "1x", "2x", "4x" ]
    cloud_spanner_sm_collection = cloud_spanner_base_collection.document(u'1x').collection(u'resources')
    cloud_spanner_med_collection = cloud_spanner_base_collection.document(u'2x').collection(u'resources')
    cloud_spanner_lrg_collection = cloud_spanner_base_collection.document(u'4x').collection(u'resources')

    i = 0
    for name in names:
        cloud_spanner_doc = cloud_spanner_base_collection.document(sizes[i]).collection(u'resources').document(name)
        cloud_spanner_doc.set({ u'expiry': 0 })
        print(" Added {} to Firestore".format(name))
        i = i + 1

    print("")
    return True

def deploy_containers(project_id, version):
    if not deploy_load_gen_script_container(project_id, version):
        return False

    if not deploy_load_gen_service_container(project_id, version):
        return False

    if not deploy_orchestrator_container(project_id):
        return False

    return True

def deploy_db_resource_container(project_id):
    proc = subprocess.run(["docker build -t breaking-db-lease ."], cwd='../db-lease-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't build the db-lease container.\n")
        print(proc.stderr)
        return False
    print(" Built the db-lease-container")

    proc = subprocess.run(["docker tag breaking-db-lease gcr.io/{}/breaking-db-lease".format(project_id)], cwd='../db-lease-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't tag the db-lease container.\n")
        print(proc.stderr)
        return False
    print(" Tagged the db-lease-container")

    proc = subprocess.run(["docker push gcr.io/{}/breaking-db-lease".format(project_id)], cwd='../db-lease-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't push the db-lease container.\n")
        print(proc.stderr)
        return False
    print(" Pushed the db-lease-container")

    print("")
    return True

def deploy_load_gen_script_container(project_id, version):
    proc = subprocess.run(["docker build -t breaking-loadgen-script ."], cwd='../load-gen-script', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't build the load-gen-script container.\n")
        print(proc.stderr)
        return False
    print(" Built the load-gen-script container")

    proc = subprocess.run(["docker tag breaking-loadgen-script gcr.io/{}/breaking-loadgen:{}".format(project_id, version)], cwd='../load-gen-script', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't tag the load-gen-script container.\n")
        print(proc.stderr)
        return False
    print(" Tagged the load-gen-script container")

    proc = subprocess.run(["docker push gcr.io/{}/breaking-loadgen".format(project_id)], cwd='../load-gen-script', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't push the load-gen-script container.\n")
        print(proc.stderr)
        return False
    print(" Pushed the load-gen-script container")

    print("")
    return True

def replace_version_string(version):
    try:
        filename = '../load-gen-service/config.yaml'
        with open(filename, 'r') as file:
            filedata = file.read()

        # Replace our version string in the config file
        filedata = re.sub(':v[0-9]+\.[0-9]+\.[0-9]+', ':{}'.format(version), filedata)

        with open(filename, 'w') as file:
            file.write(filedata)
    except:
        return False
    return True

def adjust_config_yaml(project, pubsub, version, k8s_sa):
    filename = '../load-gen-service/config.yaml'
    filedata = None
    try:
        with open("{}.example".format(filename), 'r') as file:
            filedata = file.read()
    except:
        print("Couldn't read the load gen service config yaml example file\n")
        return False

    filedata = filedata.replace("<DBName>", "test")
    filedata = filedata.replace("<DBUser>", "postgres")
    filedata = filedata.replace("<DBPassword>", "postgres")
    filedata = filedata.replace("<PubSubTopic>", pubsub)
    filedata = filedata.replace("<ProjectID>", project)
    filedata = filedata.replace("<Version>", version)
    filedata = filedata.replace("<K8SServiceAccount>", k8s_sa)

    try:
        with open(filename, 'w') as file:
            file.write(filedata)
    except:
        print("Couldn't write out the load gen service config yaml file\n")
        return False
    return True

def deploy_load_gen_service_container(project_id):
    proc = subprocess.run(["docker build -t breaking-loadgen-service ."], cwd='../load-gen-service', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't build the loadgen-service container.\n")
        print(proc.stderr)
        return False
    print("Built the loadgen-service container")

    proc = subprocess.run(["docker tag breaking-loadgen-service gcr.io/{}/breaking-loadgen-service".format(project_id)], cwd='../load-gen-service', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't tag the loadgen-service container.\n")
        print(proc.stderr)
        return False
    print("Tagged the loadgen-service container")

    proc = subprocess.run(["docker push gcr.io/{}/breaking-loadgen-service".format(project_id)], cwd='../load-gen-service', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't push the loadgen-service container.\n")
        print(proc.stderr)
        return False
    print("Pushed the loadgen-service container")

    print("")
    return True

def deploy_orchestrator_container(project_id):
    proc = subprocess.run(["docker build -t breaking-orchestrator ."], cwd='../orchestrator-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't build the orchestrator container.\n")
        print(proc.stderr)
        return False
    print("Built the orchestrator container")

    proc = subprocess.run(["docker tag breaking-orchestrator gcr.io/{}/breaking-orchestrator".format(project_id)], cwd='../orchestrator-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't tag the orchestrator container.\n")
        print(proc.stderr)
        return False
    print("Tagged the orchestrator container")

    proc = subprocess.run(["docker push gcr.io/{}/breaking-orchestrator".format(project_id)], cwd='../orchestrator-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't push the orchestrator container.\n")
        print(proc.stderr)
        return False
    print("Pushed the orchestrator container")

    print("")
    return True

def deploy_db_resource_service(service_account, region, project_id):
    proc = subprocess.run(["gcloud run deploy breaking-db-lease --platform=managed --port=5000 --allow-unauthenticated --service-account={} --region={} --image=gcr.io/{}/breaking-db-lease".format(service_account, region, project_id)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't start the db-lease Cloud Run service")
        print(proc.stderr)
        return None

    db_lease_url = None
    # I hate this...it's another case where user-useful information is being
    # put out on stderr, rather than stdout.
    out = proc.stderr
    x = re.search("percent of traffic at (.*)\n", out)
    if x != None:
        db_lease_url = x.group(1)
    if db_lease_url != None:
        print(" Started the db-lease Cloud Run service")
    return db_lease_url

def deploy_run_services(service_account, region, project_id, version):
    proc = subprocess.run(["gcloud run deploy breaking-load-service --platform=managed --port=5000 --allow-unauthenticated --service-account={} --region={} --image=gcr.io/{}/breaking-loadgen-service".format(service_account, region, project_id)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't start the load gen Cloud Run service")
        print(proc.stderr)
        return False
    print(" Started the load gen Cloud Run service")

    proc = subprocess.run(["gcloud run deploy breaking-orchestrator --platform=managed --port=5000 --allow-unauthenticated --service-account={} --region={} --image=gcr.io/{}/breaking-orchestrator".format(service_account, region, project_id)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't start the orchestrator Cloud Run service")
        print(proc.stderr)
        return False
    print(" Started the orchestrator Cloud Run service")

    print("")
    return True

def get_orchestrator_url():
    proc = subprocess.run(["gcloud run services list --platform=managed | grep breaking-orchestrator"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    url = proc.stdout.split()[3]
    return url

def deploy_k8s(region, project, vpc):
    k8s_name = "breaking-cluster"

    proc = subprocess.run(["gcloud container clusters create {} --num-nodes=5 --region={} --enable-ip-alias --workload-pool={}.svc.id.goog --network={}".format(k8s_name, region, project, vpc)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("lready exists", err)
        if not x:
            print("There was a problem creating the Kubernetes cluster")
            print(err)
            return None


    ip_proc = subprocess.run(["gcloud container clusters list | grep {}".format(k8s_name)], shell=True, capture_output=True, text=True)
    if ip_proc.returncode != 0:
        return None, None
    try:
        ip = ip_proc.stdout.split()[3]
    except:
        return None, None
    return k8s_name, ip

def verify_kubectl(ip):
    try:
        proc = subprocess.run(["kubectl cluster-info | grep {}".format(ip)], shell=True, capture_output=True, text=True, check=True)
    except:
        print("kubectl doesn't appear to be configured properly. Please configure kubectl to point at the new created cluster here: '{}' and re-run the build script".format(ip))
        return False
    return True

def adjust_k8s_service_account_yaml(service_account):
    filename = '../load-gen-script/service-account.yaml'
    k8s_account = "breaking-k8s-service-account"
    filedata = None
    try:
        with open("{}.example".format(filename), 'r') as file:
            filedata = file.read()
    except:
        print("Couldn't read the service account yaml example file\n")
        return None

    filedata = filedata.replace("<GCPServiceAccountFullName>", service_account)
    filedata = filedata.replace("<K8SServiceAccount>", k8s_account)

    try:
        with open(filename, 'w') as file:
            file.write(filedata)
    except:
        print("Couldn't write out the service account yaml file\n")
        return None

    return k8s_account

def read_k8s_service_account_yaml():
    proc = subprocess.run(["kubectl apply -f service-account.yaml"], shell=True, capture_output=True, text=True, cwd='../load-gen-script')
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("unchanged", err)
        if not x:
            print("There was a problem reading in the service account file")
            print(proc.stderr)
            return False
    return True

def bind_k8s_service_accounts(project, k8s_sa, gcp_sa):
    cmd = 'gcloud iam service-accounts add-iam-policy-binding --role roles/iam.workloadIdentityUser --member \"serviceAccount:{}.svc.id.goog[default/{}]\" {}'.format(project, k8s_sa, gcp_sa)
    proc = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Wasn't able to bind your GCP service account to the k8s service account")
        print(proc.stderr)
        return False

    # Verify that the account got bound correctly
    verify = subprocess.run(["gcloud iam service-accounts get-iam-policy {}".format(gcp_sa)], shell=True, capture_output=True, text=True)
    if verify.returncode != 0:
        print("There was a problem verifying the iam policy binding")
        print(verify.stderr)
        return False

    out = verify.stdout
    x = re.search("serviceAccount\:{}\.svc\.id\.goog\[default/{}\]".format(project, k8s_sa), out)
    if not x:
        print("Couldn't verify the iam policy binding")
        return False

    return True

def create_storage_bucket(project, region, envvar):
    bucket_name = "gs://breaking-tmp-{}/".format(int(round(time.time() * 1000)))
    proc = subprocess.run(["gsutil mb -p {} -c STANDARD -l {} -b on {}".format(project, region, bucket_name)], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("There was a problem creating the Dataflow temporary staging bucket")
        print (proc.stderr)
        return None

    # Need the os env var for the Dataflow job to read
    os.environ[envvar] = bucket_name
    return bucket_name

def deploy_dataflow():
    build_proc = subprocess.run(['mvn -e compile exec:java -Dexec.mainClass=com.google.devrel.breaking.BreakingDataTransactions -Dexec.args="--runner=DataflowRunner" 2>&1'], shell=True, capture_output=True, text=True, cwd='../dataflow-transactions')
    if build_proc.returncode != 0:
        print("There was a problem deploying the Dataflow pipeline")
        print(build_proc.stdout)
        return False

    # It doesn't show up instantly in the list, so delay some to let
    # it show up
    time.sleep(30)

    # Unfortunately, we can't rely on that return code to actually
    # reflect the PIPELINE'S state, only the Maven build state. So to
    # see if we REALLY deployed, we need to fetch the Dataflow jobs
    # and see if ours is there

    # Another caveat, is I'm not sure Beam supports the setRegion()
    # method, and if it does not, then all jobs will happen in
    # us-central1. For NOW, I'm going to assume it's safe to create a
    # unique-ish job called 'breakingdatatransactions-*' and filter on
    # that for confirmation that it's working. Later, once I confirm
    # setRegion working/not working, I can filter down further by region
    dataflow_jobname = "breakingdatatransactions"
    proc = subprocess.run(['gcloud dataflow jobs list | grep "Running\|Not Started" | grep {}'.format(dataflow_jobname)], shell=True, capture_output=True, text=True)
    # Interesting side-effect(?), when gcloud * list doesn't return any
    # results, it exits with an error code. So no need to check the results
    # of the list at all, only need the return code
    if proc.returncode != 0:
        print("Something went wrong deploying the Dataflow job")
        print(build_proc.stderr)
        return False
    return True
