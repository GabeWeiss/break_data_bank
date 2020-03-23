#!/bin/sh

# NOTE: Variable passed in is a version string, format is: 'vx.x.x'
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

projectId=$BREAKING_PROJECT_ID
#pubsubId=$BREAKING_PUBSUB_ID
if [ -z $projectId ]
then
    echo "You need to set BREAKING_PROJECT_ID and BREAKING_PUBSUB_ID to continue."
    return 1 2> /dev/null || exit 1
fi

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
