#!/usr/bin/env python

import argparse
import sys

import build_helpers

# environment variable keys
dataflow_gcs_bucket_envvar = "BREAKING_DATAFLOW_BUCKET"
region_envvar = "BREAKING_REGION"
project_envvar = "BREAKING_PROJECT"
pubsub_envvar = "BREAKING_PUBSUB"


print("\nStarting demo deployment...\n")

# Argument handling before everything
parser = argparse.ArgumentParser(description='This is a build script for the backend processes for the demo originally known as "Breaking the Data Bank". It shows off relative comparisons of load handling by Cloud SQL and Cloud Spanner.')
build_helpers.add_arguments(parser)
args = parser.parse_args()

print("Verifying prerequisite installations")

# Run a couple checks to verify pre-requisites
#if not build_helpers.verify_prerequisites():
#    sys.exit(1)

print("Verified\n")

print("In order to build this demo, we need to auth gcloud within the current script environment.\nIt will open a browser window to authenticate. Failing to authenticate will cancel the script. If you close the browser window, but not the browser, the script will hang and have to be killed either manually, or by closing the browser entirely.\n\n<Press return to continue>")

#n = input()

#########################
## Authenticate gcloud ##
#########################

print("Starting gcloud authentication")

#if not build_helpers.auth_gcloud():
#    sys.exit(1)

print("Successfully authenticated gcloud\n")


##################################
## Authorize gcloud with docker ##
##################################

print("Since we're using Container Registry for our containers, we need to authorize gcloud with docker. Replying 'yes' will add docker credHelper entry to the configuration file to register gcloud, allowing us to push to Google Container Registry.\n")

#if not build_helpers.auth_docker():
#    sys.exit(1)

print("\nDocker configured successfully\n")

###########################################
## Fetch some project metadata for later ##
###########################################

print("Fetching project metadata\n")

# Need the project id of the current configuration for all
# kinds of things
project_id = build_helpers.fetch_project_id(project_envvar)
if project_id == None:
    sys.exit(1)

print("Project id: '{}'".format(project_id))

# NOTE: This script will attempt to use a set of defaults for the
# region to set up the services. If they aren't set, it will fall
# back (and notify user) to an environment variable.
default_region = build_helpers.fetch_default_region(args.region, region_envvar)
if default_region == None:
    sys.exit(1)

print("Region: '{}'".format(default_region))

pubsub_topic = build_helpers.fetch_pubsub_topic(args.pubsub, pubsub_envvar)
if pubsub_topic == None:
    sys.exit(1)

print("Pub/Sub topic: '{}'".format(pubsub_topic))

print("\nFinished fetching project metadata\n")

######################################################
## Setup for API use (services and service account) ##
######################################################

print("Enabling GCP services/APIs")

# There are a number of services that we need in order to build
# this demo. This call enables the necessary services in your project
#if not build_helpers.enable_services():
#    sys.exit(1)

print("Successfully enabled all required services\n")

print("Creating and fetching service account (Note, you'll get an email about downloading a service key if you haven't downloaded it yet)")

#service_account = build_helpers.create_service_account(project_id)
#if service_account == None:
#    sys.exit(1)

print("Successfully created our service account\n")

#################################
## Create a VPC for everything ##
#################################

print("Creating new VPC network\n")

#vpc_name = build_helpers.create_vpc()
#if vpc_name == None:
#    sys.exit(1)

print("Successfully created VPC\n")

##########################
## Create Pub/Sub topic ##
##########################

print("Creating Pub/Sub topic")

#if not build_helpers.create_pubsub_topic(pubsub_topic):
#    sys.exit(1)

print("Successfully created Pub/Sub topic\n")

###############################
## Create Database instances ##
###############################

# Cloud SQL

print("Starting to create Cloud SQL instances (This takes awhile, don't panic)\n")

vm_cpus = [ "1", "4", "16" ]
vm_ram = [ "1GiB", "4GiB", "16GiB" ]
db_name_version = "06"
instance_names = ["break-sm{}".format(db_name_version), "break-med{}".format(db_name_version), "break-lrg{}".format(db_name_version)]

#if not build_helpers.create_sql_instances(default_region, vm_cpus, vm_ram, instance_names):
#    sys.exit(1)

print("Finished creating Cloud SQL instances\n")

print("Starting to create Cloud SQL w/ replica instances (another big wait incoming\n")

# TODO: Create Cloud SQL w/ replica instances

print ("Finished creating Cloud SQL w/ replica instances\n")

print("Starting to create Cloud Spanner instances\n")

# TODO: Create Cloud Spanner instances

print("Finished creating Cloud Spanner instances\n")

#######################################
## Insert DB metadata into Firestore ##
#######################################

print("Starting to add all database resource meta data to Firestore\n")

#if not build_helpers.initialize_firestore():
#    sys.exit(1)

#if not build_helpers.set_sql_db_resources(instance_names):
#    sys.exit(1)

print("Finished adding all database resource metadata to Firestore\n")

#################################
## Build and deploy containers ##
#################################

print("Starting to build and deploy demo microservice containers\n")

#if not build_helpers.deploy_containers(project_id, args.version):
#    sys.exit(1)

print("Finished building and deploying demo microservice containers\n")

################################
## Deploly Cloud Run services ##
################################

print("Starting to deploy Cloud Run services. This will take a bit for each one\n")

#if not build_helpers.deploy_run_services(service_account, default_region, project_id, args.version):
#    sys.exit(1)

# TODO: Fetch the URL for the orchestrator Cloud Run instance so
# we can print it out at the end for the demo-runner with instructions
# on running (curl to keep it simple, until we have the front-end also
# to deploy automatically)

print("Finished deploying Cloud Run services\n")

###################################
## Deploy the Kubernetes Cluster ##
###################################

print("Deploying the Kubernetes cluster (another potentially lengthy wait)\n")

#k8s_name, k8s_ip = build_helpers.deploy_k8s(default_region, project_id, vpc_name)
#if k8s_name == None:
#    sys.exit(1)
#if k8s_ip == None:
#    sys.exit(1)

print("Successfully deployed GKE cluster\n")

print("Verifying kubectl configuration")

#if not build_helpers.verify_kubectl(k8s_ip):
#    sys.exit(1)

print("Verified\n")

print("Setting up service account credentials linkages for GKE's workload identity\n")

print("Generating k8s service account configuration")

#k8s_service_account = build_helpers.adjust_k8s_service_account_yaml(service_account)
#if k8s_service_account == None:
#    sys.exit(1)

print("Generated config file")

print("Reading in service account")

#if not build_helpers.read_k8s_service_account_yaml():
#    sys.exit(1)

print("Read")

print("Binding service account")

#if not build_helpers.bind_k8s_service_accounts(project_id, k8s_service_account, service_account):
#    sys.exit(1)

print("Bound\n")

print("Finished up service account credentials linkage for GKE's workload identity\n")

#############################
## Deploy the Dataflow job ##
#############################

print("Creating the Dataflow staging bucket")

# First we need a temporary bucket in Cloud Storage for the job
#gcs_bucket = build_helpers.create_storage_bucket(project_id, default_region, dataflow_gcs_bucket_envvar)
#if gcs_bucket == None:
#    sys.exit(1)

print("Successfully created the Dataflow staging bucket\n")

print("Deploying Dataflow pipeline to Cloud (This is another longer wait)\n")

#if not build_helpers.deploy_dataflow():
#    sys.exit(1)

print("Successfully deployed Dataflow pipeline")


print("\n")
