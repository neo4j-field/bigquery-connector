# Neo4j BigQuery Connector

This project is a prototype of a Dataproc template to power a BigQuery
stored procedure for Apache Spark (aka Dataproc Serverless).

## Building

The code is packaged into a Docker image that gets deployed by
Dataproc onto the Spark environment.

To build:

```
$ docker build -t "gcr.io/neo4j-se-team-201905/dataproc-pyarrow:0.1.0" .
```

Then push to Google Container Registry (GCR):

```
$ docker push "gcr.io/neo4j-se-team-201905/dataproc-pyarrow:0.1.0"
```

> Note: you will need to enable your local gcloud tooling to help
> authenticate.  Try running: `gcloud auth configure-docker`

## Running

The template has been tested with AuraDS and self-managed Neo4j.

### Network Prerequisites

In either case, you most likely need to configure a GCP network to use
[Private Google Access](https://cloud.google.com/vpc/docs/private-google-access)
and possibly Cloud NAT. (Cloud NAT is definitely needed for AuraDS.)

### Submitting a Job

Using the `gcloud` tooling, use a shell script like:

```sh
#!/bin/sh
# Use fewer, larger executors
SPARK_EXE_CORES=8
SPARK_EXE_MEMORY=16g
SPARK_EXE_COUNT=2
PROPERTIES="spark.executor.cores=${SPARK_EXE_CORES}"
PROPERTIES="${PROPERTIES},spark.executor.memory=${SPARK_EXE_MEMORY}"
PROPERTIES="${PROPERTIES},spark.dynamicAllocation.initialExecutors=${SPARK_EXE_COUNT}"
PROPERTIES="${PROPERTIES},spark.dynamicAllocation.minExecutors=${SPARK_EXE_COUNT}"

gcloud dataproc batches submit pyspark \
    --region="europe-west1" \
    --version="2.0" \
    --deps-bucket="gs://neo4j_voutila" \
    --container-image="gcr.io/neo4j-se-team-201905/dataproc-pyarrow:${VERSION}" \
    --properties="${PROPERTIES}" \
    main.py -- \
    --graph_uri="gs://neo4j_voutila/gcdemo/mag240_bq.json" \
    --neo4j_host="abcdefgh.databases.neo4j.io" \
    --neo4j_password="YOUR_SUPER_SECRET_PASSWORD" \
    --bq_project="neo4j-se-team-201905" \
    --bq_dataset="gcdemo" \
    --node_tables="papers2,authors,institution" \
    --edge_tables="citations2,authorship2,affiliation" \
    --neo4j_concurrency="41"
```

The key parts to note:

1. The arguments _before_ `main.py` are specific to the PySpark job.
2. The arguments _after_ the `main.py --` are specific to the Dataproc
   template.

Customize (1) for your GCP environment and (2) for your AuraDS and
BigQuery environments as needed.

## Current Caveats

- Passwords should be stuck into _Google Secret Manager_, that part is
  on the backlog.
- All known caveats for populating GDS via Arrow Flight apply
  (e.g. node id formats, etc.).
- Concurrency doesn't auto-tune.

## Copyright and Licensing

All artifacts and code in this project, unless noted in their
respective files, are copyright Neo4j and made available under the
Apache License, Version 2.0.

No support is currently provided.
