# Breaking the Data Bank

**WARNING**: _This is not an officially support Google product._ 

This repo holds the back-end code for a demo built for Google Cloud Next 2020's data management and databases showcase.

It uses many Google Cloud products:
[Cloud SQL](https://cloud.google.com/sql)
[Cloud Spanner](https://cloud.google.com/spanner)
[Cloud Run](https://cloud.google.com/run)
[Container Registry](https://cloud.google.com/container-registry)
[Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine)
[Pub/Sub](https://cloud.google.com/pubsub)
[Dataflow](https://cloud.google.com/dataflow)
[Firestore](https://cloud.google.com/firestore)

The architectural flow for the demo goes:

![Architecture Diagram](/images/architecture.svg)

Front End (in our case the demo front-end, which isn't in this repo, is built in [Angular](https://angularjs.org/)) issues an HTTP POST request  to a Python Quart server (Quart is an async module that uses Flask), called the 'orchestrator'. Orchestrator

Private repository for a Next '2020 showcase.
