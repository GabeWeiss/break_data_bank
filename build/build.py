#!/usr/bin/env python

import argparse
import sys

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
flag_verify_prerequisites = 0
flag_authenticate_gcloud = 0 # we may never need this because we're using a service account
flag_authorize_gcloud_docker = 0
flag_enable_gcp_services = 0
flag_setup_and_fetch_service_account = 0
flag_setup_firestore = 0
flag_create_vpc = 0
flag_create_pubsub = 0
flag_deploy_db_lease_service = 0
flag_create_db_instances = 0
flag_add_dbs_to_firestore = 1
flag_build_and_deploy_containers = 0
flag_deploy_cloud_run_services = 0
flag_deploy_k8s_cluster = 0
flag_deploy_dataflow = 0

print("\nStarting demo deployment...\n")

# Argument handling before everything
parser = argparse.ArgumentParser(description='This is a build script for the backend processes for the demo originally known as "Breaking the Data Bank". It shows off relative comparisons of load handling by Cloud SQL and Cloud Spanner.')
build_helpers.add_arguments(parser)
args = parser.parse_args()

if flag_verify_prerequisites:
    print("Verifying prerequisite installations")

    # Run a couple checks to verify pre-requisites
    if not build_helpers.verify_prerequisites():
        sys.exit(1)

    print("  Verified\n")

if flag_authenticate_gcloud:
    print("In order to build this demo, we need to auth gcloud within the current script environment.\nIt will open a browser window to authenticate. Failing to authenticate will cancel the script. If you close the browser window, but not the browser, the script will hang and have to be killed either manually, or by closing the browser entirely.\n\n<Press return to continue>")

    n = input()

    #########################
    ## Authenticate gcloud ##
    #########################

    print("Starting gcloud authentication")

    if not build_helpers.auth_gcloud():
        sys.exit(1)

    print("  Successfully authenticated gcloud\n")


##################################
## Authorize gcloud with docker ##
##################################

if flag_authorize_gcloud_docker:
    print("Since we're using Container Registry for our containers, we need to authorize gcloud with docker. Replying 'yes' will add docker credHelper entry to the configuration file to register gcloud, allowing us to push to Google Container Registry.\n")

    if not build_helpers.auth_docker():
        sys.exit(1)

    print("  Docker configured successfully\n")

###########################################
## Fetch some project metadata for later ##
###########################################

print("Fetching project metadata\n")

# Need the project id of the current configuration for all
# kinds of things
project_id = build_helpers.fetch_project_id(project_envvar)
if project_id == None:
    sys.exit(1)

print(" Project id: '{}'".format(project_id))

# NOTE: This script will attempt to use a set of defaults for the
# region to set up the services. If they aren't set, it will fall
# back (and notify user) to an environment variable.
sql_region = build_helpers.fetch_sql_region(args.region, region_envvar)
if sql_region == None:
    sys.exit(1)

print(" Region: '{}'".format(sql_region))

# We try to determine a good spanner region based on the region specified
# to keep them close to the same area
spanner_region = build_helpers.extrapolate_spanner_region(sql_region)
if spanner_region == None:
    sys.exit(1)

print(" Spanner region: '{}'".format(spanner_region))

# We will grab an available App Engine region (for Firestore) based as closely
# on the region specified by the compute region we've fetched already
firestore_region = build_helpers.extrapolate_firestore_region(sql_region)
if firestore_region == None:
    sys.exit(1)

print(" Firestore region: '{}'".format(firestore_region))

pubsub_topic = build_helpers.fetch_pubsub_topic(args.pubsub, pubsub_envvar)
if pubsub_topic == None:
    sys.exit(1)

print(" Pub/Sub topic: '{}'\n".format(pubsub_topic))

print("  Finished fetching project metadata\n")

######################################################
## Setup for API use (services and service account) ##
######################################################

if flag_enable_gcp_services:
    print("Enabling GCP services/APIs (there are a lot of them, this will take some time)")

    # There are a number of services that we need in order to build
    # this demo. This call enables the necessary services in your project
    if not build_helpers.enable_services():
        sys.exit(1)

    print("  Successfully enabled all required services\n")

if flag_setup_and_fetch_service_account:
    print("Creating and fetching service account (Note, you'll get an email about downloading a service key if you haven't downloaded it yet)")

    service_account = build_helpers.create_service_account(project_id)
    if service_account == None:
        sys.exit(1)

    print("  Successfully created our service account\n")

#######################################################
## Verify they've setup Firestore, and initialize DB ##
#######################################################

if flag_setup_firestore:
    print("Setting up Firestore")

    if not build_helpers.initialize_firestore(project_id, firestore_region):
        sys.exit(1)

    print("Firestore all setup to go\n")

#################################
## Create a VPC for everything ##
#################################

if flag_create_vpc:
    print("Creating new VPC network")

    vpc_name = build_helpers.create_vpc()
    if vpc_name == None:
        sys.exit(1)

    print("  Successfully created VPC\n")

##########################
## Create Pub/Sub topic ##
##########################

if flag_create_pubsub:
    print("Creating Pub/Sub topic")

    if not build_helpers.create_pubsub_topic(pubsub_topic):
        sys.exit(1)

    print("  Successfully created Pub/Sub topic\n")

###############################
## Create Database instances ##
###############################

if flag_deploy_db_lease_service:
    # First we need to deploy our db-lease service because it manages adding our db metadata
    # to the Firestore instance

    print("Deploying database lease container\n")

    if not build_helpers.deploy_db_resource_container(project_id):
        sys.exit(1)

    print("  Successfully deployed database lease container\n")

    print("Deploying database lease Cloud Run service\n")

    #db_resource_url = build_helpers.deploy_db_resource_service(service_account, sql_region, project_id)
    if db_resource_url == None:
        sys.exit(1)

    print("  Successfully deployed database lease service\n")

db_name_version = "3"
# Note, for the meta data insertion to work, the name of the instance must
# have "sm" in it for db_size = 1, "med" for db_size = 2, and "lrg" for
# db_size = 3.
instance_names = ["break-sm{}".format(db_name_version), "break-med{}".format(db_name_version), "break-lrg{}".format(db_name_version)]

if flag_create_db_instances:
    # Cloud SQL

    print("Starting to create Cloud SQL instances (This takes awhile, don't panic)\n")

    vm_cpus = [ "1", "4", "16" ]
    vm_ram = [ "1GiB", "4GiB", "16GiB" ]

    if not build_helpers.create_sql_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc_name):
        sys.exit(1)

    print("  Finished creating Cloud SQL instances\n")

    print("Starting to create Cloud SQL w/ replica instances (another big wait incoming\n")

    if not build_helpers.create_sql_replica_instances(sql_region, vm_cpus, vm_ram, instance_names, vpc_name):
        sys.exit(1)

    print ("  Finished creating Cloud SQL w/ replica instances\n")

    print("Starting to create Cloud Spanner instances\n")

    power_unit = [ "1", "4", "10" ]
    spanner_descriptions = [ "Breaking Small{}".format(db_name_version), "Breaking Medium{}".format(db_name_version), "Breaking Large{}".format(db_name_version) ]

    if not build_helpers.create_spanner_instances(instance_names, spanner_region, power_unit, spanner_descriptions):
        sys.exit(1)

    print("  Finished creating Cloud Spanner instances\n")


#######################################
## Insert DB metadata into Firestore ##
#######################################

if flag_add_dbs_to_firestore:
    print("Starting to add all database resource meta data to Firestore\n")
    # TODO: adjust the Firestore indexes programatically:
    # https://cloud.google.com/firestore/docs/query-data/indexing#use_the_firebase_cli
    # Will need to also change up the prerequisites to include the firebase cli
    # Could also look into auto-installing them by prompt perhaps
 
 # DEBUGGING (this gets set up above when we deploy the db-lease service)
    db_resource_url = "https://breaking-db-lease-5gh6m2f5oq-uc.a.run.app"
 # Remove line above when ready to deploy for real

    db_add_endpoint = "{}/add".format(db_resource_url)
    if not build_helpers.set_sql_db_resources(instance_names, db_add_endpoint):
        sys.exit(1)

    if not build_helpers.set_spanner_db_resources(instance_names, db_add_endpoint):
        sys.exit(1)

    print("  Finished adding all database resource metadata to Firestore\n")


#############################################
## Build and deploy rest of the containers ##
#############################################

if flag_build_and_deploy_containers:
    # We need to create our k8s service account yaml first, because
    # we need the service account name in our container's yaml configuration
    # This call ONLY creates the yaml file, it doesn't do anything with it
    # until after the containers are deployed
    print(" Generating k8s service account configuration")

    k8s_service_account = build_helpers.adjust_k8s_service_account_yaml(service_account)
    if k8s_service_account == None:
        sys.exit(1)

    print("   Generated config file")

    print("Starting to build and deploy demo microservice containers\n")

    if not build_helpers.deploy_containers(project_id, args.version):
        sys.exit(1)

    print("  Finished building and deploying demo microservice containers\n")

################################
## Deploly Cloud Run services ##
################################

if flag_deploy_cloud_run_services:
    print("Creating the config yaml file for the load gen service")

    if not build_helpers.adjust_config_yaml(project_id, pubsub_topic, args.version, k8s_service_account):
        sys.exit(1)

    print("  Created\n")

    print("Starting to deploy Cloud Run services. This will take a bit for each one\n")

    if not build_helpers.deploy_run_services(service_account, sql_region, project_id, args.version):
        sys.exit(1)

    # Ultimately we need this for the end-user of the demo
    orchestrator_url = build_helpers.get_orchestrator_url()
    if orchestrator_url == None:
        sys.exit(1)

    print("  Finished deploying Cloud Run services\n")

###################################
## Deploy the Kubernetes Cluster ##
###################################

if flag_deploy_k8s_cluster:
    print("Deploying the Kubernetes cluster (another potentially lengthy wait)")

    k8s_name, k8s_ip = build_helpers.deploy_k8s(sql_region, project_id, vpc_name)
    if k8s_name == None:
        sys.exit(1)
    if k8s_ip == None:
        sys.exit(1)

    print("  Successfully deployed GKE cluster\n")

    print("Verifying kubectl configuration")

    if not build_helpers.verify_kubectl(k8s_ip):
        sys.exit(1)

    print("  Verified\n")

    print("Setting up service account credentials linkages for GKE's workload identity\n")

    print(" Reading in k8s service account")

    if not build_helpers.read_k8s_service_account_yaml():
        sys.exit(1)

    print("   Read")

    print(" Binding service account")

    if not build_helpers.bind_k8s_service_accounts(project_id, k8s_service_account, service_account):
        sys.exit(1)

    print("   Bound\n")

    print("  Finished up service account credentials linkage for GKE's workload identity\n")

#############################
## Deploy the Dataflow job ##
#############################

if flag_deploy_dataflow:
    print("Creating the Dataflow staging bucket")

    # First we need a temporary bucket in Cloud Storage for the job
    gcs_bucket = build_helpers.create_storage_bucket(project_id, sql_region, dataflow_gcs_bucket_envvar)
    if gcs_bucket == None:
        sys.exit(1)

    print("  Successfully created the Dataflow staging bucket\n")

    print("Deploying Dataflow pipeline to Cloud (This is another longer wait)")

    if not build_helpers.deploy_dataflow():
        sys.exit(1)

    print("  Successfully deployed Dataflow pipeline")


print("\n")
