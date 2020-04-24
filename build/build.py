#!/usr/bin/env python

import argparse
import os
import sys
import time

import build_helpers

# environment variable keys
dataflow_gcs_bucket_envvar = "BREAKING_DATAFLOW_BUCKET"
region_envvar = "BREAKING_REGION"
project_envvar = "BREAKING_PROJECT"
pubsub_envvar = "BREAKING_PUBSUB"


# Feature flags to enable/disable different parts of this build script
# Broadly speaking, this is for debugging purposes, and you shouldn't
# change any of these values. If you don't wish to do any of the pieces,
# set the variable to 0
flag_verify_prerequisites                = 1
flag_authenticate_gcloud                 = 1
flag_authorize_gcloud_docker             = 1
flag_enable_gcp_services                 = 1
flag_setup_and_fetch_service_account     = 1
flag_setup_firestore                     = 1
flag_create_vpc                          = 1
flag_create_pubsub                       = 1
flag_deploy_db_lease_service             = 1
flag_create_db_instances                 = 1
flag_add_dbs_to_firestore                = 1
flag_build_and_deploy_loadgen_containers = 1
flag_build_and_deploy_orchestrator       = 1
flag_deploy_cloud_run_services           = 1
flag_deploy_k8s_cluster                  = 1
flag_deploy_dataflow                     = 1

# Argument handling before everything
parser = argparse.ArgumentParser(description='This is a build script for the backend processes for the demo originally known as "Breaking the Data Bank". It shows off relative comparisons of load handling by Cloud SQL and Cloud Spanner.')
build_helpers.add_arguments(parser)
args = parser.parse_args()

run_cached = args.cached

print("\nStarting demo deployment...")
if run_cached:
    print("Setting up in cached mode.\n")
else:
    print("Setting up in full run mode.\n")

# First validate the region they want to run the demo in
if not build_helpers.validate_region(args.region):
    sys.exit(1)

# To track how long things are taking
time_tracker = 0
current_time = lambda: (int(round(time.time() * 1000))) / 1000.0
time_start = current_time()

if flag_verify_prerequisites:
    print("Verifying prerequisite installations")
    time_tracker = current_time()

    # Run a couple checks to verify pre-requisites
    if not build_helpers.verify_prerequisites():
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Verified in {time_tracker} seconds\n")

if flag_authenticate_gcloud:
    print("In order to build this demo, we need to auth two CLI applications: gcloud, and firebase CLI within the current script environment.\nIt will open a browser window, once for each service, to authenticate. Failing to authenticate will cancel the script. If you close either browser windows, but not the browser fully, the script will hang and have to be killed either manually, or by closing the browser entirely.\n\n<Press return to continue>")

    n = input()

    #########################
    ## Authenticate gcloud ##
    #########################

    print("Starting gcloud authentication")

    if not build_helpers.auth_gcloud():
        sys.exit(1)

    print("  Successfully authenticated gcloud\n")

    print("Starting firebase CLI authentication")

    if not build_helpers.auth_firebaseCLI():
        sys.exit(1)

    print("  Successfully authenticated firebase CLI\n")


##################################
## Authorize gcloud with docker ##
##################################

if flag_authorize_gcloud_docker:
    print("Configuring Docker and gcloud to play nicely together.")

    time_tracker = current_time()
    if not build_helpers.auth_docker():
        sys.exit(1)
    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Docker configured successfully in {time_tracker} seconds\n")

###########################################
## Fetch some project metadata for later ##
###########################################

print("Fetching project metadata\n")

time_tracker = current_time()
# Need the project id of the current configuration for all
# kinds of things
project_id = build_helpers.fetch_project_id(project_envvar)
if project_id == None:
    sys.exit(1)

print(f" Project id: '{project_id}'")

# Regions are tricky. Different services have different regions in place.
# To that end, I'm allowing users of the demo to specify "usa", "emea" or
# "apac" and I'll set regions accordingly.
sql_region, app_region, firestore_region, spanner_region, run_region, gke_region, storage_region = build_helpers.assign_regions(args.region)

# Set environment variable for regional Dataflow pickup
os.environ[region_envvar] = storage_region

print(f" Regions:\n  Cloud SQL: '{sql_region}'\n  AppEngine: '{app_region}'\n  Firestore: '{firestore_region}'\n  Spanner: '{spanner_region}'\n  Cloud Run: '{run_region}'\n  GKE: '{gke_region}'\n  Storage: '{storage_region}'")

pubsub_topic = build_helpers.fetch_pubsub_topic(args.pubsub, pubsub_envvar)
if pubsub_topic == None:
    sys.exit(1)

print(f" Pub/Sub topic: '{pubsub_topic}'\n")

time_tracker = round(current_time() - time_tracker, 2)
print(f"  Finished fetching project metadata in {time_tracker} seconds\n")

######################################################
## Setup for API use (services and service account) ##
######################################################

if flag_enable_gcp_services:
    print("Enabling GCP services/APIs (there are a lot of them, this will take some time)")
    time_tracker = current_time()

    # There are a number of services that we need in order to build
    # this demo. This call enables the necessary services in your project
    if not build_helpers.enable_services(run_cached):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully enabled all required services in {time_tracker} seconds\n")

if flag_setup_and_fetch_service_account:
    print("Creating and fetching service account (Note, you'll get an email about downloading a service key if you haven't downloaded it yet)")
    time_tracker = current_time()

    service_account = build_helpers.create_service_account(project_id)
    if service_account == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully created our service account in {time_tracker} seconds\n")

#######################################################
## Verify they've setup Firestore, and initialize DB ##
#######################################################

if flag_setup_firestore:
    print("Setting up Firestore")
    time_tracker = current_time()

    if not build_helpers.initialize_firestore(project_id, app_region, firestore_region, run_cached):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Firestore setup in {time_tracker} seconds\n")

#################################
## Create a VPC for everything ##
#################################

if flag_create_vpc and not run_cached:
    print("Creating new VPC network")
    time_tracker = current_time()

    vpc_name = build_helpers.create_vpc(project_id)
    if vpc_name == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully created VPC in {time_tracker} seconds\n")

##########################
## Create Pub/Sub topic ##
##########################

if flag_create_pubsub and not run_cached:
    print("Creating Pub/Sub topic")
    time_tracker = current_time()

    if not build_helpers.create_pubsub_topic(pubsub_topic):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully created Pub/Sub topic in {time_tracker} seconds\n")

###############################
## Create Database instances ##
###############################

if flag_deploy_db_lease_service and not run_cached:
    # First we need to deploy our db-lease service because it manages adding our db metadata
    # to the Firestore instance
    print("Deploying database lease container\n")
    time_tracker = current_time()

    if not build_helpers.deploy_db_resource_container(project_id):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully deployed database lease container in {time_tracker} seconds\n")

    print("Deploying database lease Cloud Run service\n")
    time_tracker = current_time()

    db_resource_url = build_helpers.deploy_db_resource_service(service_account, run_region, project_id)
    if db_resource_url == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully deployed database lease service in {time_tracker} seconds\n")

db_name_version = "1"
# Note, for the meta data insertion to work, the name of the instance must
# have "sm" in it for db_size = 1, "med" for db_size = 2, and "lrg" for
# db_size = 3.
instance_names = [ f"break-sm{db_name_version}", f"break-med{db_name_version}", f"break-lrg{db_name_version}" ]

if flag_create_db_instances and not run_cached:
    # Cloud SQL

    print("Starting to create Cloud SQL instances (This takes awhile, don't panic)\n")
    time_tracker = current_time()

    vm_cpus = [ "1", "4", "16" ]
    vm_ram = [ "1GiB", "4GiB", "16GiB" ]

    if not build_helpers.create_sql_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc_name):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished creating Cloud SQL instances in {time_tracker} seconds\n")

    print("Starting to create Cloud SQL w/ replica instances (another big wait incoming\n")
    time_tracker = current_time()

    if not build_helpers.create_sql_replica_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc_name):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print (f"  Finished creating Cloud SQL w/ replica instances in {time_tracker} seconds\n")

    print("Starting to create Cloud Spanner instances\n")
    time_tracker = current_time()

    power_unit = [ "1", "4", "10" ]
    spanner_descriptions = [ f"Breaking Small{db_name_version}", f"Breaking Medium{db_name_version}", f"Breaking Large{db_name_version}" ]

    if not build_helpers.create_spanner_instances(instance_names, spanner_region, power_unit, spanner_descriptions):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished creating Cloud Spanner instances in {time_tracker} seconds\n")


#######################################
## Insert DB metadata into Firestore ##
#######################################

if flag_add_dbs_to_firestore and not run_cached:
    print("Starting to add all database resource meta data to Firestore\n")
    time_tracker = current_time()

    db_add_endpoint = f"{db_resource_url}/add"
    if not build_helpers.set_sql_db_resources(instance_names, db_add_endpoint):
        sys.exit(1)

    if not build_helpers.set_spanner_db_resources(instance_names, db_add_endpoint):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished adding all database resource metadata to Firestore in {time_tracker} seconds\n")


#############################################
## Build and deploy rest of the containers ##
#############################################

if flag_build_and_deploy_loadgen_containers and not run_cached:
    # We need to create our k8s service account yaml first, because
    # we need the service account name in our container's yaml configuration
    # This call ONLY creates the yaml file, it doesn't do anything with it
    # until after the containers are deployed
    print(" Generating k8s service account configuration")
    time_tracker = current_time()

    k8s_service_account = build_helpers.adjust_k8s_service_account_yaml(service_account)
    if k8s_service_account == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"   Generated config file in {time_tracker} seconds")

    print("Starting to build and deploy loadgen microservice containers\n")
    time_tracker = current_time()

    if not build_helpers.deploy_loadgen_containers(project_id, args.version):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished building and deploying loadgen microservice containers in {time_tracker} seconds\n")

if flag_build_and_deploy_orchestrator:
    print("Starting to build and deploy orchestrator microservice containers\n")
    time_tracker = current_time()

    if not build_helpers.deploy_orchestrator_container(project_id):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished building and deploying orchestrator microservice containers in {time_tracker} seconds\n")


################################
## Deploly Cloud Run services ##
################################

orchestrator_url = None
if flag_deploy_cloud_run_services:
    if not run_cached:
        print("Creating the config yaml file for the load gen service")
        time_tracker = current_time()

        if not build_helpers.adjust_config_yaml(project_id, pubsub_topic, args.version, k8s_service_account):
            sys.exit(1)

        time_tracker = round(current_time() - time_tracker, 2)
        print(f"  Created in {time_tracker} seconds\n")

    print("Starting to deploy Cloud Run services. This will take a bit for each one\n")
    time_tracker = current_time()

    if not run_cached:
        if not build_helpers.deploy_loadgen_run_services(service_account, run_region, project_id, args.version):
            sys.exit(1)

    if not build_helpers.deploy_orchestrator_run_services(service_account, run_region, project_id):
            sys.exit(1)

    # Ultimately we need this for the end-user of the demo
    orchestrator_url = build_helpers.get_orchestrator_url()
    if orchestrator_url == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished deploying Cloud Run services in {time_tracker} seconds\n")

###################################
## Deploy the Kubernetes Cluster ##
###################################

if flag_deploy_k8s_cluster and not run_cached:
    print("Deploying the Kubernetes cluster (another potentially lengthy wait)")
    time_tracker = current_time()

    k8s_name, k8s_ip = build_helpers.deploy_k8s(gke_region, project_id, vpc_name)
    if k8s_name == None:
        sys.exit(1)
    if k8s_ip == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully deployed GKE cluster in {time_tracker} seconds\n")

    print("Verifying kubectl configuration")
    time_tracker = current_time()

    if not build_helpers.verify_kubectl(k8s_ip):
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Verified in {time_tracker} seconds\n")

    print("Setting up service account credentials linkages for GKE's workload identity\n")
    time_tracker = current_time()

    print(" Reading in k8s service account")

    if not build_helpers.read_k8s_service_account_yaml():
        sys.exit(1)

    print("   Read")

    print(" Binding service account")

    if not build_helpers.bind_k8s_service_accounts(project_id, k8s_service_account, service_account):
        sys.exit(1)

    print("   Bound\n")

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Finished up service account credentials linkage for GKE's workload identity in {time_tracker} seconds\n")

#############################
## Deploy the Dataflow job ##
#############################

if flag_deploy_dataflow and not run_cached:
    print("Creating the Dataflow staging bucket")
    time_tracker = current_time()

    # First we need a temporary bucket in Cloud Storage for the job
    gcs_bucket = build_helpers.create_storage_bucket(project_id, storage_region, dataflow_gcs_bucket_envvar)
    if gcs_bucket == None:
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully created the Dataflow staging bucket in {time_tracker} seconds\n")

    print("Deploying Dataflow pipeline to Cloud (This is another longer wait)")
    time_tracker = current_time()

    if not build_helpers.deploy_dataflow():
        sys.exit(1)

    time_tracker = round(current_time() - time_tracker, 2)
    print(f"  Successfully deployed Dataflow pipeline in {time_tracker} seconds\n")


########################
## Final Instructions ##
########################

time_stop = round(current_time() - time_start, 2)
print(f"The build script has successfully completed.\nIt took a total of {time_stop} seconds.\n\nThe entry point for any front-end is our orchestrator service, which is currently running at:\n\n{orchestrator_url}\n\nThe APIs for that entrance point can be found in the API_REFERENCE.txt file in the repo's root folder.")

print("\n")

if not build_helpers.cleanup():
    print("\nWasn't able to cleanup something, just be sure to be sure not to commit anything you shouldn't somewhere.\n")
