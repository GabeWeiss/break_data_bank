#!/usr/bin/env python

import argparse
import json
import os
import re
import requests
from shutil import copyfile
import subprocess
import time
import webbrowser

# These values appear in several of the service scripts
# If they change here, they must be changed everywhere
CLOUD_SQL = 1
CLOUD_SQL_READ_REPLICA = 2
CLOUD_SPANNER = 3

# These 3 are used by both the configuration yaml file, as well as
# the envvars for the db-lease service
DATABASE_NAME = "test"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "postgres"

def add_arguments(parser_obj):
    parser_obj.add_argument("-v", "--version", required=True,help="This is the version specified for the load-gen-script container. Should be in the format 'vx.x.x', e.g. v0.0.2")
    parser_obj.add_argument("-r", "--region", help="Specify a region to create your Cloud Run instances")
    parser_obj.add_argument("-p", "--pubsub", help="Specifies a pub/sub topic, defaults to 'breaking-demo-topic'", default="breaking-demo-topic")
    parser_obj.add_argument("-c", "--cached", help="Sets up the demo to run in the cached mode instead of really running against the DBs", action='store_true')

def verify_prerequisites():
    try:
        subprocess.run(["docker --version"], shell=True, check=True, capture_output=True)
        subprocess.run(["kubectl version --client"], shell=True, check=True, capture_output=True)
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

def validate_region(region):
    if region == "usa":
        return True
    elif region == "emea":
        return True
    elif region == "apac":
        return True

    print("The three valid values for the demo's region are:\n usa\n emea\n apac")
    return False

# return is: 
#   sql_region
#   app_region
#   firestore_region
#   spanner_region
#   run_region
#   gke_region
#   storage_region (also Dataflow region)
def assign_regions(region):
    if region == "usa":
        return "us-west2", "us-west2", "us-west2", "nam6", "us-west1", "us-west2", "us-west1"
    elif region == "emea":
        return "europe-west1", "europe-west2", "eur3", "eur3", "europe-west1", "europe-west1", "europe-west1"
    elif region == "apac":
        return "asia-east2", "asia-east2", "asia-east2", "nam-eur-asia1", "asia-east1", "asia-east2", "asia-east1"
    else:
        return None

def auth_gcloud():
    try:
        subprocess.run(["gcloud auth login", "--brief"], shell=True, check=True, capture_output=True)
    except:
        print("Couldn't authenticate gcloud")
        return False
    return True

def auth_firebaseCLI():
    try:
        subprocess.run(["firebase login"], shell=True, check=True, capture_output=True)
    except:
        print("Couldn't authenticate firebase CLI")
        return False
    return True

def auth_docker():
    try:
        subprocess.run(["gcloud auth configure-docker"], shell=True, check=True)
    except:
        print("There was a problem authorizing gcloud with docker\n")
        return False
    return True

def enable_services(run_cached):
    services = "run.googleapis.com iam.googleapis.com sqladmin.googleapis.com container.googleapis.com firestore.googleapis.com pubsub.googleapis.com dataflow.googleapis.com containerregistry.googleapis.com spanner.googleapis.com sql-component.googleapis.com storage-component.googleapis.com servicenetworking.googleapis.com compute.googleapis.com"
    cached_services = "run.googleapis.com iam.googleapis.com container.googleapis.com firestore.googleapis.com containerregistry.googleapis.com compute.googleapis.com"
    if run_cached:
        services = cached_services

    services_process = subprocess.run([f"gcloud services enable {services}"], shell=True, capture_output=True, text=True)
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
    sa_process = subprocess.run([f"gcloud iam service-accounts create {sa_name} --display-name \"{sa_name} service account\""], shell=True, capture_output=True, text=True)
    if sa_process.returncode != 0:
        err = sa_process.stderr
        x = re.search("is the subject of a conflict", err)
        if not x:
            print("There was a problem creating the service account")
            print (err)
            return None

    full_name = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
        # We need firebase and datastore at higher levels because
        # Firestore doesn't have gcloud support, so we need to do
        # everything via APIs rather than gcloud for it
    sa_roles = [ "cloudsql.client", "firebase.admin", "datastore.owner", "spanner.databaseAdmin", "dataflow.admin", "run.invoker", "container.admin", "pubsub.publisher" ]
    for role in sa_roles:
        proc = subprocess.run([f"gcloud projects add-iam-policy-binding {project_id} --member serviceAccount:{full_name} --role roles/{role}"], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr
            x = re.search("is the subject of a conflict", err)
            if not x:
                print("There was a problem assigning roles to the service account")
                print (err)
                return None

    current_dir = os.path.dirname(os.getcwd())
    json_path = f"{current_dir}/breaking-service-account.json"

    if not os.path.exists(json_path):
        print("Downloading service account bearer token")
        proc = subprocess.run([f"gcloud iam service-accounts keys create {json_path} --iam-account {full_name}"], shell=True, capture_output=True, text=True)
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
def create_vpc(project_id):
    vpc_name = "breaking-vpc"
    proc = subprocess.run([f"gcloud compute networks create {vpc_name}"], shell=True, capture_output=True, text=True)
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
    proc = subprocess.run([f"gcloud compute addresses create {allocation_name} --global --purpose=VPC_PEERING --prefix-length=24 --network={vpc_name}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("already exists", err)
        if not x:
            print("Wasn't able to allocate private IPs for your VPC network")
            print(err)
            return None

    proc = subprocess.run([f"gcloud services vpc-peerings update --service=servicenetworking.googleapis.com  --network={vpc_name}     --project={project_id} --ranges={allocation_name} --force"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("already exists", err)
        if not x:
            print("Wasn't able to connect private services")
            print(err)
            return None

    return vpc_name

def fetch_project_id():
    project_id_process = subprocess.run(["gcloud config get-value project"], shell=True, capture_output=True, text=True)
    if project_id_process.stdout != "":
        proj = project_id_process.stdout.rstrip()
        return proj

    print("Something went wrong with authorization with gcloud. Please try again and be sure to authorize when it pops up in your browser.")
    return None

def fetch_pubsub_topic(pubsub, project_id):
    # We shouldn't ever have None here since there's a default
    # value in the args parsing, so it'll be an error condition
    if pubsub == None:
        print("   Something really bad happened fetching the Pub/Sub topic because it's None")
        return None

    return pubsub

def create_pubsub_topic(pubsub_topic):
    proc = subprocess.run([f"gcloud pubsub topics create {pubsub_topic}"], shell=True, capture_output=True, text=True)
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
            db_create_process = subprocess.run([f"gcloud beta sql instances create {name} --database-version=POSTGRES_11 --region={sql_region} --no-backup --assign-ip --root-password=postgres --tier=db-f1-micro --network={vpc}"], shell=True, capture_output=True, text=True)
        else:
            db_create_process = subprocess.run([f"gcloud beta sql instances create {name} --database-version=POSTGRES_11 --region={sql_region} --no-backup --assign-ip --root-password=postgres --cpu={cpu} --memory={ram} --network={vpc}"], shell=True, capture_output=True, text=True)

        if db_create_process.returncode != 0:
            err = db_create_process.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print(f"There was a problem creating instance: '{name}'")
                print (err)
                return False

        print(f" Instance '{name}' created with '{cpu}' CPU(s) and '{ram}' RAM.")
        i = i + 1

    print("")
    return True

def create_sql_replica_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc):
    i = 0
    for short_name in instance_names:
        name = f"{short_name}-r"
        cpu = vm_cpus[i]
        ram = vm_ram[i]

        # Need to special case our initial instance because we
        # want it to be weak intentionally, and custom machines
        # aren't weak enough...so using the --tier flag to create
        # a micro instance (.6 GiB RAM and 1 vCPU) as our first instace
        db_create_process = None
        if cpu == "1":
            db_create_process = subprocess.run([f"gcloud beta sql instances create {name} --database-version=POSTGRES_11 --region={sql_region} --no-backup --assign-ip --root-password=postgres --tier=db-f1-micro --network={vpc}"], shell=True, capture_output=True, text=True)
        else:
            db_create_process = subprocess.run([f"gcloud beta sql instances create {name} --database-version=POSTGRES_11 --region={sql_region} --no-backup --assign-ip --root-password=postgres --cpu={cpu} --memory={ram} --network={vpc}"], shell=True, capture_output=True, text=True)

        if db_create_process.returncode != 0:
            err = db_create_process.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print(f"There was a problem creating instance: '{name}'")
                print (err)
                return False

            # NOTE: There's a workaround in here for b/145025740
            # I've removed all references to --tier, --ram and --cpu. This means,
            # currently, it should grab the tier of the master instance which
            # is fine. We SHOULD be able to specify different specs for the replica
            # than the master, but currently we can't
        replica_name = f"{name}-replica"
        replica_process = subprocess.run([f"gcloud beta sql instances create {replica_name} --database-version=POSTGRES_11 --region={sql_region} --no-backup --assign-ip --root-password=postgres --network={vpc} --master-instance-name={name}"], shell=True, capture_output=True, text=True)
        
        if replica_process.returncode != 0:
            err = replica_process.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print(f"There was a problem creating instance: '{name}'")
                print (err)
                return False

        print(f" Instance '{name}' created with '{cpu}' CPU(s) and '{ram}' RAM. With read replica: '{replica_name}'")
        i = i + 1

    print("")
    return True

def create_spanner_instances(instance_names, spanner_region, nodes, spanner_descriptions):
    i = 0
    for name in instance_names:
        node_count = nodes[i]
        description = spanner_descriptions[i]
        proc = subprocess.run([f"gcloud spanner instances create {name} --nodes={node_count} --config={spanner_region} --description=\"{description}\""], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            err = proc.stderr
                # This particular error is given when the instance
                # already exists, in which case, for our purposes, we
                # won't exit the script, we can assume they ran before
                # and perhaps something went wrong, so they're retrying
            x = re.search("is the subject of a conflict", err)
            if not x:
                print(f"There was a problem creating instance: '{name}'")
                print (err)
                return False

        print(f" Spanner instance '{name}' created with {node_count} nodes")
        i = i + 1

    print("")
    return True

def run_firestore_create(region):
    # Alpha has a create database in gcloud, we'll go ahead and dogfood this...
    proc = subprocess.run([f"gcloud alpha firestore databases create --region={region}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        return proc.stderr

    return None

def initialize_firestore(project_id, app_region, firestore_region, run_cached):
    create_err = run_firestore_create(firestore_region)
    if create_err != None:
        x = re.search("You must first create an Google App Engine app", create_err)
        if not x:
            print("   There was a problem creating the Firestore database.")
            print(proc.stderr)
            return False

        proc = subprocess.run([f"gcloud app create --region={app_region}"], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            print("   Wasn't able to create our default app engine application to enable Firestore database creation.")
            print(proc.stderr)
            return False
        
        # Try one more time now that we have our app engine created
        second_chance_err = run_firestore_create(firestore_region)
        if second_chance_err != None:
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
    proc = subprocess.run([f"firebase projects:addfirebase {project_id}"], shell=True, text=True, capture_output=True, cwd=firestore_dir)
    if proc.returncode != 0:
        debug_proc = subprocess.run(["tail -n 20 firebase-debug.log"], shell=True, text=True, capture_output=True, cwd=firestore_dir)
        if debug_proc.returncode != 0:
            print("   Something went wrong trying to get the debug output from the firebase CLI error")
            print(debug_proc.stderr)
            return False
        out = debug_proc.stdout
        x = re.search("is already a Firebase project", out)
        y = re.search("Requested entity already exists", out)
        if not x and not y:
            print("   Wasn't able to add Firestore initialization to the environment")
            print(out)
            return False

    if not run_cached:
#        print("\nThis next section may ask you to pick names for certain resources. Go ahead and just hit enter and leave the defaults the way they are.\n")
#        print("Press return to continue...")
#        y = input()

#        proc = subprocess.run([f"firebase init firestore -P {project_id}"], shell=True, text=True, cwd=firestore_dir)
#        if proc.returncode != 0:
#            print("   Wasn't able to initialize firestore for our project.")
#            print(proc.stderr)
#            return False

        try:
            copyfile('firestore.indexes', f"{firestore_dir}/firestore.indexes.json")
            copyfile('firebase_json', f"{firestore_dir}/firebase.json")
            copyfile('firestore_rules', f"{firestore_dir}/firestore.rules")
        except:
            print("   Wasn't able to copy our firebase files")
            return False

        proc = subprocess.run(["firebase deploy --only firestore"], shell=True, text=True, capture_output=True, cwd=firestore_dir)
        if proc.returncode != 0:
            print("   There was a problem creating our Firestore indexes.")
            print(proc.stderr)
            return False

    return True

def add_db(url, database_type, database_size, resource_id, connection_string, replica_ip):
    parameters = {'database_type':database_type,'database_size':database_size,'resource_id':resource_id, 'connection_string':connection_string, 'replica_ip':replica_ip }
    r = requests.post(url = url, json = parameters)

    # TODO: Figure out a better error code system for the db-lease container

    db_name = "cloud sql"
    if database_type == CLOUD_SQL_READ_REPLICA:
        db_name = "cloud sql w/ replica"
    elif database_type == CLOUD_SPANNER:
        db_name = "cloud spanner"
    print(f" Added {db_name} instance of size {database_size}")
    return True

# Make use of the db-lease service to add our dbs to Firestore
    # Service needs:
    #   "database_type"
    #   "database_size"
    #   "resource_id"
    #   "connection_string"
    #   "replica_ip" <-- when DB is type CLOUD_SQL_READ_REPLICA
def set_sql_db_resources(names, url, project_id, region):
    INST_POS_RESOURCE_ID = 0
    INST_POS_IP = 5

    for name in names:
        database_size = -1
        database_type = CLOUD_SQL # Starts here
        if re.search("sm", name):
            database_size = 1
        elif re.search("med", name):
            database_size = 2
        elif re.search("lrg", name):
            database_size = 3

        if database_size == -1:
            print(f"   Couldn't figure out what size our database is ({name}), so it won't be added to our Firestore metadata.")
            return False

        resource_id = None
        connection_string = None
        replica_ip = None

        # First we handle Cloud SQL
        info_proc = subprocess.run([f"gcloud sql instances list | grep \"{name} \""], shell=True, capture_output=True, text=True)
        if info_proc.returncode != 0:
            print("   There was a problem fetching the main sql instance info")
            print(info_proc.stderr)
            return False
        out = info_proc.stdout

        instance_data = out.split()
        try:
            resource_id = f"{project_id}:{region}:{instance_data[INST_POS_RESOURCE_ID]}"
            connection_string = instance_data[INST_POS_IP]
        except:
            print("   Looks like the format of the gcloud sql instances list output changed")
            return False

        if not add_db(url, database_type, database_size, resource_id,       connection_string, None):
            return False

        # Next we handle the replica
        master_proc = subprocess.run([f"gcloud sql instances list | grep \"{name}-r \""], shell=True, capture_output=True, text=True)
        if master_proc.returncode != 0:
            print("   There was a problem fetching the replica info")
            print(master_proc.stderr)
            return False
        master_out = master_proc.stdout

        replica_proc = subprocess.run([f"gcloud sql instances list | grep \"{name}-r-replica \""], shell=True, capture_output=True, text=True)
        if replica_proc.returncode != 0:
            print("   There was a problem fetching the replica info")
            print(replica_proc.stderr)
            return False
        replica_out = replica_proc.stdout

        master_data = master_out.split()
        replica_data = replica_out.split()

        database_type = CLOUD_SQL_READ_REPLICA
        try:
            resource_id = f"{project_id}:{region}:{master_data[INST_POS_RESOURCE_ID]}"
            connection_string = master_data[INST_POS_IP]
            replica_ip = replica_data[INST_POS_IP]
        except:
            print("   Wasn't able to get our replica data. The data format might have changed")
            return False

        if not add_db(url, database_type, database_size, resource_id,       connection_string, replica_ip):
            return False

    return True

def set_spanner_db_resources(names, url):
    database_type = CLOUD_SPANNER
    for name in names:
        database_size = -1
        if re.search("sm", name):
            database_size = 1
        elif re.search("med", name):
            database_size = 2
        elif re.search("lrg", name):
            database_size = 3

        proc = subprocess.run([f"gcloud spanner instances list | grep {name}"], shell=True, capture_output=True, text=True)
        if proc.returncode != 0:
            print("   Wasn't able to fetch our spanner instance list")
            print(proc.stderr)
            return False

        out = proc.stdout
        try:
            resource_id = out.split()[0]
            connection_string = resource_id
        except:
            print("   Wasn't able to fetch out spanner IP, the format of gcloud spanner instances list may have changed")
            return False

        if not add_db(url, database_type, database_size, resource_id,       connection_string, None):
            return False

    print("")
    return True

def deploy_loadgen_containers(project_id, version):
    if not deploy_load_gen_script_container(project_id, version):
        return False

    if not deploy_load_gen_service_container(project_id):
        return False

    return True

def deploy_orchestrator_container(project_id):
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

    proc = subprocess.run([f"docker tag breaking-db-lease gcr.io/{project_id}/breaking-db-lease"], cwd='../db-lease-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't tag the db-lease container.\n")
        print(proc.stderr)
        return False
    print(" Tagged the db-lease-container")

    proc = subprocess.run([f"docker push gcr.io/{project_id}/breaking-db-lease"], cwd='../db-lease-container', shell=True, capture_output=True, text=True)
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

    proc = subprocess.run([f"docker tag breaking-loadgen-script gcr.io/{project_id}/breaking-loadgen:{version}"], cwd='../load-gen-script', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't tag the load-gen-script container.\n")
        print(proc.stderr)
        return False
    print(" Tagged the load-gen-script container")

    proc = subprocess.run([f"docker push gcr.io/{project_id}/breaking-loadgen"], cwd='../load-gen-script', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't push the load-gen-script container.\n")
        print(proc.stderr)
        return False
    print(" Pushed the load-gen-script container")

    print("")
    return True

def adjust_config_yaml(project, pubsub, version, k8s_sa):
    filename = '../load-gen-service/config.yaml'
    filedata = None
    try:
        with open(f"{filename}.example", 'r') as file:
            filedata = file.read()
    except:
        print("Couldn't read the load gen service config yaml example file\n")
        return False

    filedata = filedata.replace("<DBName>", DATABASE_NAME)
    filedata = filedata.replace("<DBUser>", DATABASE_USER)
    filedata = filedata.replace("<DBPassword>", DATABASE_PASSWORD)
    filedata = filedata.replace("<PubSubTopic>", pubsub)
    filedata = filedata.replace("<ProjectID>", project)
    filedata = filedata.replace("<Version>", version)
    filedata = filedata.replace("<K8SServiceAccount>", k8s_sa)

    try:
        with open(filename, 'w') as file:
            file.write(filedata)
            file.close()
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

    proc = subprocess.run([f"docker tag breaking-loadgen-service gcr.io/{project_id}/breaking-loadgen-service"], cwd='../load-gen-service', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't tag the loadgen-service container.\n")
        print(proc.stderr)
        return False
    print("Tagged the loadgen-service container")

    proc = subprocess.run([f"docker push gcr.io/{project_id}/breaking-loadgen-service"], cwd='../load-gen-service', shell=True, capture_output=True, text=True)
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

    proc = subprocess.run([f"docker tag breaking-orchestrator gcr.io/{project_id}/breaking-orchestrator"], cwd='../orchestrator-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't tag the orchestrator container.\n")
        print(proc.stderr)
        return False
    print("Tagged the orchestrator container")

    proc = subprocess.run([f"docker push gcr.io/{project_id}/breaking-orchestrator"], cwd='../orchestrator-container', shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Couldn't push the orchestrator container.\n")
        print(proc.stderr)
        return False
    print("Pushed the orchestrator container")

    print("")
    return True

# Ret values represent ready, and fail states. E.g. False, False means
# the indexes are not ready, but there was no errors in retrieving the state.
# False, True means there was a problem getting the state information
# True, False means all indexes have been created and there were no errors
# True, True is right out. That's just weird.
def are_firestore_indexes_ready(sleep_time):
    proc = subprocess.run([f"gcloud alpha firestore indexes composite list --format=flattened"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("There was a problem fetching the firestore composite indexes")
        print(proc.stderr)
        return False, True

    out = proc.stdout
    entries = out.split('---')
    for entry in entries:
        collection_match = re.search("\/collectionGroups\/resources\/indexes", entry)

        if not collection_match:
            #print(f"---\n{entry}\---\n")
            #print("Didn't match collection 'resources', moving on\n")
            continue

        state_match = re.search("state\:[\s]+([A-Z]+)\n", entry)
        if not state_match:
            print("Couldn't match our index state.")
            continue

        state = state_match.group(1)
        if state == "CREATING":
            return False, False

    proc = subprocess.run([f"gcloud alpha firestore indexes fields list --format=flattened"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("There was a problem fetching the firestore field indexes")
        print(proc.stderr)
        return False, True

    out = proc.stdout
    entries = out.split('---')
    for entry in entries:
        collection_match = re.search("\/collectionGroups\/resources\/fields", entry)

        if not collection_match:
            #print(f"---\n{entry}\---")
            #print("Didn't match collection 'resources', moving on\n\n")
            continue

        # There's several lines in the fields indexes that could all be CREATING
        # so we need to break down (yes, I know, n^2 it's fine)
        lines = entry.split("\n")
        for line in lines:
            state_match = re.search("state\:[\s]+([A-Z]+)\n", entry)
            if not state_match:
                print("Couldn't match our index state.")
                continue

            state = state_match.group(1)
            if state == "CREATING":
                return False, False

    return True, False

def deploy_db_resource_service(service_account, region, project_id, sql_names, sql_region):
    service_name = "breaking-db-lease"
    sleep_time = 10 # start at a 10 second retry
    ready = False
    # While our firestore indexes aren't ready, we're going to do an
    # exponential backoff retry on checking for 5 minutes. If we go
    # past that, go ahead and fail for real
    while ready == False:
        ready, fail = are_firestore_indexes_ready(sleep_time)
        if fail:
            return None

        if ready == False:
            print(f"Firestore indexes aren't ready yet, waiting {sleep_time} seconds to try again.")
            time.sleep(sleep_time)
            sleep_time = sleep_time * 2
            # Set currently to 10s, 20s, 40s, 1:20, 2:40, 5:20, 12:00
            # If it takes longer than that, error out.
            if sleep_time > 720:
                print("   Firstore indexes never appear to have gotten set. You can check manually for when they're done by running:\n\ngcloud alpha firestore indexes composite list --format=flattened\n\nAnd you should get back something that doesn't look like two empty values. If you don't see that, you should be able to check the progress in the console or by re-running that command. Once it's all set, re-run the script and it should get past this point.\n")
                return None

    # We need to generate the names for the SQL instances
    # to pass to the gcloud run deploy command to link the
    # Cloud SQL instances so they can connect for cleaning
    instances_param = ""
    for name in sql_names:
        line = f"{project_id}:{sql_region}:{name},{project_id}:{sql_region}:{name}-r,"
        instances_param = f"{instances_param}{line}"
    # Strip off the final comma, we don't need it
    instances_param = instances_param[:-1]

    proc = subprocess.run([f"gcloud run deploy {service_name} --platform=managed --port=5000 --allow-unauthenticated --service-account={service_account} --region={region} --update-env-vars=DB_NAME={DATABASE_NAME},DB_USER={DATABASE_USER},DB_PASSWORD={DATABASE_PASSWORD},PROD=1 --image=gcr.io/{project_id}/breaking-db-lease --add-cloudsql-instances={instances_param}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't start the db-lease Cloud Run service")
        print(proc.stderr)
        return None

    db_lease_url = None
    proc = subprocess.run([f"gcloud run services list --platform managed | grep {service_name}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't fetch our service url")
        return None
    out = proc.stdout
    x = re.search("(http[0-9a-z\.\-\/\:]*)", out)
    if x != None:
        db_lease_url = x.group(1)
    if db_lease_url != None:
        print(" Started the db-lease Cloud Run service")

    return db_lease_url

def deploy_loadgen_run_services(service_account, run_region, project_id, version):
    proc = subprocess.run([f"gcloud run deploy breaking-load-service --platform=managed --port=5000 --allow-unauthenticated --service-account={service_account} --region={run_region} --image=gcr.io/{project_id}/breaking-loadgen-service"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("   Couldn't start the load gen Cloud Run service")
        print(proc.stderr)
        return False
    print(" Started the load gen Cloud Run service")

    return True

def deploy_orchestrator_run_services(service_account, run_region, project_id):
    proc = subprocess.run([f"gcloud run deploy breaking-orchestrator --platform=managed --port=5000 --allow-unauthenticated --service-account={service_account} --region={run_region} --image=gcr.io/{project_id}/breaking-orchestrator"], shell=True, capture_output=True, text=True)
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
        print("   Couldn't fetch our orchestrator URL")
        return None
    url = proc.stdout.split()[3]
    return url


# TODO: This k8s_name also needs to be updated in the container for the
# load-gen-webserver/base.py if changed
def deploy_k8s(region, project, vpc):
    k8s_name = "breaking-cluster"

    proc = subprocess.run([f"gcloud container clusters create {k8s_name} --num-nodes=10 --region={region} --enable-ip-alias --workload-pool={project}.svc.id.goog --network={vpc}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr
        x = re.search("lready exists", err)
        if not x:
            print("There was a problem creating the Kubernetes cluster")
            print(err)
            return None


    ip_proc = subprocess.run([f"gcloud container clusters list | grep {k8s_name}"], shell=True, capture_output=True, text=True)
    if ip_proc.returncode != 0:
        return None, None
    try:
        ip = ip_proc.stdout.split()[3]
    except:
        return None, None
    return k8s_name, ip

def verify_kubectl(ip):
    try:
        proc = subprocess.run([f"kubectl cluster-info | grep {ip}"], shell=True, capture_output=True, text=True, check=True)
    except:
        print(f"kubectl doesn't appear to be configured properly. Please configure kubectl to point at the new created cluster here: '{ip}' and re-run the build script")
        return False
    return True

def adjust_k8s_service_account_yaml(service_account):
    filename = '../load-gen-script/service-account.yaml'
    k8s_account = "breaking-k8s-service-account"
    filedata = None
    try:
        with open(f"{filename}.example", 'r') as file:
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
    cmd = f'gcloud iam service-accounts add-iam-policy-binding --role roles/iam.workloadIdentityUser --member \"serviceAccount:{project}.svc.id.goog[default/{k8s_sa}]\" {gcp_sa}'
    proc = subprocess.run([cmd], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("Wasn't able to bind your GCP service account to the k8s service account")
        print(proc.stderr)
        return False

    # Verify that the account got bound correctly
    verify = subprocess.run([f"gcloud iam service-accounts get-iam-policy {gcp_sa}"], shell=True, capture_output=True, text=True)
    if verify.returncode != 0:
        print("There was a problem verifying the iam policy binding")
        print(verify.stderr)
        return False

    out = verify.stdout
    x = re.search(f"serviceAccount\:{project}\.svc\.id\.goog\[default/{k8s_sa}\]", out)
    if not x:
        print("Couldn't verify the iam policy binding")
        return False

    return True

def create_storage_bucket(project, region):
    time_hash = int(round(time.time() * 1000))
    bucket_name = f"gs://breaking-tmp-{time_hash}/"
    proc = subprocess.run([f"gsutil mb -p {project} -c STANDARD -l {region} -b on {bucket_name}"], shell=True, capture_output=True, text=True)
    if proc.returncode != 0:
        print("There was a problem creating the Dataflow temporary staging bucket")
        print (proc.stderr)
        return None

    return bucket_name

def deploy_dataflow(service_account, project_id, region, gcp_bucket, pubsub_topic):
    # Need the full pubsub topic, so generate it here
    full_pubsub = f'projects/{project_id}/topics/{pubsub_topic}'
    build_proc = subprocess.run([f'mvn -e compile exec:java -Dexec.mainClass=com.google.devrel.breaking.BreakingDataTransactions -Dexec.args="--runner=DataflowRunner --serviceAccount={service_account} --project={project_id} --region={region} --gcpTempLocation={gcp_bucket}tmp --pubsubTopic={full_pubsub}" 2>&1'], shell=True, capture_output=True, text=True, cwd='../dataflow-transactions')
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
    proc = subprocess.run([f'gcloud dataflow jobs list | grep "Running\|Not Started" | grep {dataflow_jobname}'], shell=True, capture_output=True, text=True)
    # Interesting side-effect(?), when gcloud * list doesn't return any
    # results, it exits with an error code. So no need to check the results
    # of the list at all, only need the return code
    if proc.returncode != 0:
        print("Something went wrong deploying the Dataflow job")
        print(build_proc.stderr)
        return False
    return True

def cleanup():
    print("CLEANING UP")
