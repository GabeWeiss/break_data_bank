#!/usr/bin/env python

import argparse
import os
import re
import sys

import build_helpers

parser = argparse.ArgumentParser(description='This is a build script for the backend processes for the demo originally known as "Breaking the Data Bank". It shows off relative comparisons of load handling by Cloud SQL and Cloud Spanner.')

build_helpers.add_arguments(parser)

args = parser.parse_args()

print("\nStarting demo deployment...\n")

print("In order to build this demo, we need to auth gcloud within the current script environment.\nIt will open a browser window to authenticate. Failing to authenticate will cancel the script. If you close the browser window, but not the browser, the script will hang and have to be killed either manually, or by closing the browser entirely.\n\n<Press return to continue>")

n = input()

#########################
## Authenticate gcloud ##
#########################

print("Starting gcloud authentication")

success = build_helpers.auth_gcloud()
if not success:
    sys.exit(1)

print("Successfully authenticated gcloud\n")


##################################
## Authorize gcloud with docker ##
##################################

print("Since we're using Container Registry for our containers, we need to authorize gcloud with docker. Replying 'yes' will add docker credHelper entry to the configuration file to register gcloud, allowing us to push to Google Container Registry.\n")

success = build_helpers.auth_docker()
if not success:
    sys.exit(1)

print("\nDocker configured successfully\n")

###########################################
## Fetch some project metadata for later ##
###########################################

print("Fetching project ID")

# Need the project id of the current configuration for all
# kinds of things
project_id = build_helpers.fetch_project_id()
if project_id == None:
    sys.exit(1)

print("Retrieved project id: '{}'\n".format(project_id))

print("Fetching region")

# NOTE: This script will attempt to use a set of defaults for the
# region to set up the services. If they aren't set, it will fall
# back (and notify user) to an environment variable.
default_region = build_helpers.fetch_default_region(args.region)
if default_region == None:
    sys.exit(1)

print("Retrieved region: '{}'\n".format(default_region))


######################################################
## Setup for API use (services and service account) ##
######################################################

print("Enabling GCP services/APIs")

# There are a number of services that we need in order to build
# this demo. This call enables the necessary services in your project
success = build_helpers.enable_services()
if not success:
    sys.exit(1)

print("Successfully enabled all required services\n")

print("Creating and fetching service account (Note, you'll get an email about downloading a service key if you haven't downloaded it yet)")

service_account = build_helpers.create_service_account(project_id)
if service_account == None:
    sys.exit(1)

print("Successfully created our service account\n")


###############################
## Create Database instances ##
###############################

# Cloud SQL

print("Starting to create Cloud SQL instances (This takes awhile, don't panic)\n")

vm_cpus = [ "1", "4", "16" ]
vm_ram = [ "1GiB", "4GiB", "16GiB" ]
db_name_version = "06"
instance_names = ["break-sm{}".format(db_name_version), "break-med{}".format(db_name_version), "break-lrg{}".format(db_name_version)]

success = build_helpers.create_sql_instances(default_region, vm_cpus, vm_ram, instance_names)
if not success:
    sys.exit(1)

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

success = build_helpers.initialize_firestore()
if not success:
    sys.exit(1)

success = build_helpers.set_sql_db_resources(instance_names)
if not success:
    sys.exit(1)

print("Finished adding all database resource metadata to Firestore\n")

#################################
## Build and deploy containers ##
#################################

print("Starting to build and deploy demo microservice containers\n")

success = build_helpers.deploy_containers(project_id, args.version)
if not success:
    sys.exit(1)

print("Finished building and deploying demo microservice containers\n")

################################
## Deploly Cloud Run services ##
################################

print("Starting to deploy Cloud Run services. This will take a bit for each one\n")

success = build_helpers.deploy_run_services(service_account, default_region, project_id, args.version)
if not success:
    sys.exit(1)

print("Finished deploying Cloud Run services\n")
