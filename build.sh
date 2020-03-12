#!/bin/sh

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
docker tag breaking-loadgen-script gcr.io/$projectId/breaking-loadgen
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
    echo "Wasn't able to build the orchestrator"
    return 1 2> /dev/null || exit 1
fi
docker tag breaking-orchestrator gcr.io/$projectId/orchestrator
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to tag the orchestrator"
    return 1 2> /dev/null || exit 1
fi
docker push gcr.io/$projectId/orchestrator
if [ $? -ne 0 ]
then
    popd
    echo "Wasn't able to push the orchestrator"
    return 1 2> /dev/null || exit 1
fi
# Finished the orchestrator script build and push
popd
