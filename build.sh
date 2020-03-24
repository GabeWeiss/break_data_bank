#!/bin/sh

# NOTE: Cloud Run containers need a service account 
# with appropriate permissions to run. To this end,
# you'll need a service account created with the following
# permissions:
# - Cloud SQL Client
# - Dataflow Admin
# - Firebase Develop Admin
# - Pub/Sub Editor
# - Kubernetes Engine Admin
# Then take the name of the service account, and set it
# to the environment variable DEMO_SERVICE_ACCOUNT before
# running this script
serviceAccount=$DEMO_SERVICE_ACCOUNT
if [ -z $serviceAccount ]
then
    echo ""
    echo "You need to have a service account created and set. Please check the comment at the top of this build script for directions."
    echo ""
fi

# NOTE: This script will attempt to use a set of defaults for the
# region to set up the services. If they aren't set, it will fall
# back (and notify user) to an environment variable.
defaultRegion=$(gcloud config get-value run/region 2> /dev/null)
if [ -z "$defaultRegion" ]
then
    defaultRegion=$(gcloud config get-value compute/region 2> /dev/null)
    if [ -z "$defaultRegion" ]
    then
        defaultRegion=$DEMO_REGION
    fi
fi

# if we still have no defaultRegion then all our efforts failed
# and tell them they need to set the variables so I can pick it up
if [ -z "$defaultRegion" ]
then
    echo ""
    echo "Wasn't able to determine a region to start our Cloud Run services."
    echo "Please either set a region using 'gcloud config set run/region <region>'"
    echo "or set an environment variable 'DEMO_REGION' with the name of the region."
    echo "Ensure that the region is a valid one. Regions can be found by running"
    echo "'gcloud compute regions list'."
    echo ""
    return 1 2> /dev/null || exit 1
fi


# NOTE: The script needs authorization on the current project
# where you'll be deploying things. If you don't have permission
# as your user to do all the things for the demo, you need to
# find a user that is for deploying purposes. These permissions are:
# - Cloud SQL Client
# - Dataflow Admin
# - Firebase Develop Admin
# - Pub/Sub Editor
# - Kubernetes Engine Admin
# (There may be more, I'll add them here when I get told about problems)
gcloud auth login --brief
projectId=$(gcloud config get-value project)

# NOTE: Positional variable passed in is a version string, format is: 'vx.x.x'
# Version string only applies to the load-gen-script container because
# the rest will update fine in the Cloud Run instances. load-gen-script
# is picked up as a k8s job by the load-gen-server, and cached, so we
# need to use a different version number to ensure that the proper
# version of the script is being used.
if [ -z "$1" ]
then
    echo "Need to pass in a version number in the format vn.n.n, e.g. v0.0.1."
    echo "Please check the config.yaml file in the load-gen-service directory for the current set version."
    return 1 2> /dev/null || exit 1
fi

if [ -z $projectId ]
then
    echo "Something went wrong with authorization with gcloud. Please try again and be sure to authorize when it pops up in your browser."
    return 1 2> /dev/null || exit 1
fi

##############################################
## Building and deploying containers to GCR ##
##############################################

# Building and deploying the db-lease container
pushd db-lease-container
docker build -t breaking-db-lease .
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to build the db-lease-container"
    return 1 2> /dev/null || exit 1
fi

docker tag breaking-db-lease gcr.io/$projectId/breaking-db-lease
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to tag the db-lease-container"
    return 1 2> /dev/null || exit 1
fi

docker push gcr.io/$projectId/breaking-db-lease
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to push the db-lease-container"
    return 1 2> /dev/null || exit 1
fi
# Finished db-lease build and push
popd

# Build and push load gen script
pushd load-gen-script
docker build -t breaking-loadgen-script .
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to build the loadgen-script"
    return 1 2> /dev/null || exit 1
fi
docker tag breaking-loadgen-script gcr.io/$projectId/breaking-loadgen:$1
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to tag the loadgen-script"
    return 1 2> /dev/null || exit 1
fi
docker push gcr.io/$projectId/breaking-loadgen
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to push the loadgen-script"
    return 1 2> /dev/null || exit 1
fi
# Finished the loadgen script build and push
popd

# Build and push load gen service
pushd load-gen-service
sed -i '' "s#\:v[0-9]\.[0-9]\.[0-9]*#\:$1#" config.yaml
docker build -t breaking-loadgen-service .
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to build the loadgen-service"
    return 1 2> /dev/null || exit 1
fi
docker tag breaking-loadgen-service gcr.io/$projectId/breaking-loadgen-service
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to tag the loadgen-service"
    return 1 2> /dev/null || exit 1
fi
docker push gcr.io/$projectId/breaking-loadgen-service
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to push the loadgen-service"
    return 1 2> /dev/null || exit 1
fi
# Finished the loadgen service build and push
popd

# Build and push load orchestrator
pushd orchestrator-container
docker build -t breaking-orchestrator .
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to build the breaking-orchestrator"
    return 1 2> /dev/null || exit 1
fi
docker tag breaking-orchestrator gcr.io/$projectId/breaking-orchestrator
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to tag the breaking-orchestrator"
    return 1 2> /dev/null || exit 1
fi
docker push gcr.io/$projectId/breaking-orchestrator
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to push the breaking-orchestrator"
    return 1 2> /dev/null || exit 1
fi
# Finished the breaking-orchestrator script build and push
popd



############################
## Deploying to Cloud Run ##
############################

# db-lease Cloud Run service
gcloud run deploy breaking-db-lease --platform=managed --port=5000 --allow-unauthenticated --service-account=$serviceAccount --region=$defaultRegion --image=gcr.io/$projectId/breaking-db-lease

if [ $? -ne 0 ]
then
    popd
    echo "Couldn't deploy the db-lease Cloud Run service"
    return 1 2> /dev/null || exit 1
fi

# load-gen-service Cloud Run service
gcloud run deploy breaking-load-service --platform=managed --port=5000 --allow-unauthenticated --service-account=$serviceAccount --region=$defaultRegion --image=gcr.io/$projectId/breaking-loadgen-service

if [ $? -ne 0 ]
then
    popd
    echo "Couldn't deploy the load-gen-service Cloud Run service"
    return 1 2> /dev/null || exit 1
fi

# orchestrator Cloud Run service
gcloud run deploy breaking-orchestrator --platform=managed --port=5000 --allow-unauthenticated --service-account=$serviceAccount --region=$defaultRegion --image=gcr.io/$projectId/breaking-orchestrator

if [ $? -ne 0 ]
then
    popd
    echo "Couldn't deploy the orchestrator Cloud Run service"
    return 1 2> /dev/null || exit 1
fi
