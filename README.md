# Breaking the Data Bank

**WARNING**: _This is not an officially support Google product._ 

This repo holds the back-end code for a demo built for Google Cloud Next 2020's data management and databases showcase. The demo visualizes average transaction time and transaction failure percentages over time for Cloud SQL (*db_type: 1*), Cloud SQL with a Read Replica (*db_type: 2*), and Cloud Spanner (*db_type: 3*) instances of various sizes. It's built to run three different patterns of traffic at three different intensity levels. Each run hits the databases for about thirty seconds.

**Traffic Patterns**
- 1: Consistent low frequency
- 2: Consistent high frequency
- 3: Spikey (alternates low and high frequency)

It uses many Google Cloud products:
- [Cloud SQL](https://cloud.google.com/sql)
- [Cloud Spanner](https://cloud.google.com/spanner)
- [Cloud Run](https://cloud.google.com/run)
- [Container Registry](https://cloud.google.com/container-registry)
- [Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine)
- [Pub/Sub](https://cloud.google.com/pubsub)
- [Dataflow](https://cloud.google.com/dataflow)
- [Firestore](https://cloud.google.com/firestore)

The architectural flow for the demo goes:

![Architecture Diagram](/images/architecture.png)

Front End (the demo front-end, which isn't in this repo, is built in [Angular](https://angularjs.org/)) issues an HTTP POST request to a [Python Quart](https://gitlab.com/pgjones/quart) server (Quart is an async module that uses [Flask](https://flask.palletsprojects.com/en/1.1.x/)), called the 'orchestrator' which runs on Cloud Run ([Orchestrator code](/orchestrator-container)).

There are three types of requests the orchestrator accepts:

*/cached*, */fail* and */run*

Cached will fetch previously calculated values, while fail and run will spin up the full pipeline.

See [Sample curl examples](/sample_curls.txt) for variable requirements for each endpoint.

The cached endpoint returns a specific key to a document, which when combined with the desired traffic pattern gives the location where the cached results are stored. For example: If you request results for Cloud SQL (db_type: 1) of size 2 (db_size: 2) of intensity 3 (intensity: 3), with a low consistent read pattern (read_pattern: 1) and a spiky write_pattern (write_pattern: 3), the result from /cached would come back as "sql-2-3". Which means, for our demo, the path in Firestore to the collection which contains our cached data would be:

*/events/next2020/cached/sql-2-3/patterns/1-3/transactions*

This was done because it was the closest way to implement a cached response compared to our full runs.

The full runs will return a job_id which is a hash, for example: "0Tz4u0VuW2c4E1rJY1W7", which can be used to build the path to a collection where the transactions will be put:

*/events/next2020/transactions/0Tz4u0VuW2c4E1rJY1W7/transactions*

In the cached run, the front-end retrieves the transactions with *stream()* on the collection, while in the full runs, the front-end would subscribe to updates on the collection path and build the graph to update live as data comes in.

So on a cached run, Orchestrator simply returns the key for the Firestore path and exits out.

For full runs, it's a bit more complicated.

Orchestrator calls out to another Quart service running in Cloud Run which manages our database resources so we don't run multiple runs of the demo against the same databases (which would of course affect our results). The *db_lease* service ([code](/db-lease-container)) stores metadata on deployed instances. This service returns back a connection string if one is available of the requested type. It then "leases" the database, marking it unavailable for a certain amount of time.

Orchestrator then takes this connection string and sends it to a third Quart service in Cloud Run called the "loadgen service" ([code](/load-gen-service)). The loadgen service then pulls a container we've put in the Container Registry, which contains our load generating scripts ([code](/load-gen-script)), and deploys it as a Kubernetes job to a cluster we've deployed, passing in all the metadata we've accumulated along the way to define the specific load job we're doing.

The load generating script hammers the database assigned to it with a combination of reads and writes with the passed in read and write patterns and intensity. Intensity is determined by number of nodes in Kubernetes assigned to the job when it's run. While the load generating pods are doing their thing, the script tracks individual transaction times and whether it succeeded, and passes batches of those data bundles to a Pub/Sub topic.

A deployed streaming Dataflow job ([code](/dataflow-transactions)) pulls from the Pub/Sub topic, and aggregates the transaction data into one second windows, and writes the averaged results to the Firestore instance at the above specified path.

A [background script](/db-lease-container/db_lease/db_clean.py) running in the db_lease container on a timer gathers up any leased databases, and once their lease period expires, cleans the databases and resets them back to a starting state.

Please feel free to take a look and ask questions or file issues against this repo.