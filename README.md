# Neo4j BigQuery Connector

This project is a prototype of a Dataproc template to power a BigQuery
stored procedure for Apache Spark (aka Dataproc Serverless).

## Building

The code is packaged into a Docker image that gets deployed by
Dataproc onto the Spark environment.

To build:

```
$ docker build -t "gcr.io/your-gcp-project/neo4j-bigquery-connector:0.3.0" .
```

Then push to Google Container Registry (GCR):

```
$ docker push "gcr.io/your-gcp-project/neo4j-bigquery-connector:0.3.0"
```

> Note: you will need to enable your local gcloud tooling to help
> authenticate.  Try running: `gcloud auth configure-docker`

## Running

The template has been tested with AuraDS and self-managed Neo4j.

### Network Prerequisites

In either case, you most likely need to configure a GCP network to use
[Private Google Access](https://cloud.google.com/vpc/docs/private-google-access)
and possibly Cloud NAT. (Cloud NAT is definitely needed for AuraDS.)

### Submitting a Dataproc Serverless Job

If you're looking to just use the Dataproc capabilities or looking to
do some quick testing, you can submit a batch job directly to
Dataproc.

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
    --deps-bucket="gs://your-bucket" \
    --container-image="gcr.io/your-gcp-project/neo4j-bigquery-connector:0.3.0" \
    --properties="${PROPERTIES}" \
    main.py -- \
    --graph_uri="gs://your-bucket/folder/model.json" \
    --neo4j_secret="projects/123456/secrets/neo4j-bigquery/versions/1" \
    --bq_project="your-gcp-project" \
    --bq_dataset="your_bq_dataset" \
    --node_tables="papers,authors,institution" \
    --edge_tables="citations,authorship,affiliation"
```

The key parts to note:

1. The arguments _before_ `main.py` are specific to the PySpark job.
2. The arguments _after_ the `main.py --` are specific to the Dataproc
   template.

Customize (1) for your GCP environment and (2) for your AuraDS and
BigQuery environments as needed.

> Note: you can put configuration values in a JSON document stored in
> a Google Secret Manager secret (that's a mouthful). Us the
> `--neo4j_secret` parameter to pass in the full resource id (which
> should include the secret version number).

## Configuring a Google BigQuery Stored Procedure

In short, you'll want to familiarize yourself with the [Stored
procedures for Apache
Spark](https://cloud.google.com/bigquery/docs/spark-procedures)
documentation. Assuming you've got your environment properly
configured and enrolled in the preview program to use Spark for stored
procedures, you need to create your stored procedure. For example:

```
CREATE OR REPLACE PROCEDURE
  `my-gcp-project.my_bigquery_dataset.neo4j_gds_graph_project`(
    graph_name STRING,
    graph_uri STRING,
    neo4j_secret STRING,
    bq_project STRING,
    bq_dataset STRING,
    node_tables ARRAY<STRING>,
    edge_tables ARRAY<STRING>)
WITH CONNECTION `your-gcp-project.eu.my-spark-connection` OPTIONS (
    engine='SPARK',
    runtime_version='2.0',
    container_image='eu.gcr.io/your-gcp-project/neo4j-bigquery-connector:0.3.0',
    properties=[],
    description="Project a graph from BigQuery into Neo4j AuraDS or GDS.")
LANGUAGE python AS R"""

import sys
print(f"original path: {sys.path}")
newpath = [p for p in sys.path if not "spark-bigquery-support" in p]
sys.path = newpath
print(f"new path: {sys.path}")

from pyspark.sql import SparkSession
from templates import BigQueryToNeo4jGDSTemplate

spark = (
      SparkSession
      .builder
      .appName("Neo4j BigQuery Connector")
      .getOrCreate()
)

template = BigQueryToNeo4jGDSTemplate()
args = template.parse_args()
template.run(spark, args)

""";
```

Some details on the inputs:

- `graph_name` -- the resulting name of the graph projection in AuraDS
- `graph_uri` -- the GCS uri pointing to a JSON file describing the
  [graph model](https://github.com/neo4j-field/dataflow-flex-pyarrow-to-gds#the-graph-model)
  for your data
- `neo4j_secret` -- a Google Secret Manager secret resource id
  containing a JSON blob with additional arguments:
  * `neo4j_user` -- name of the Neo4j user to connect as
  * `neo4j_password` -- password for the given user
  * `neo4j_host` -- hostname (_not Bolt uri_) of the AuraDS instance
- `bq_project` -- GCP project id owning the BigQuery source data
- `bq_dataset` -- BigQuery dataset name for the source data
- `node_tables` -- an `ARRAY<STRING>` of BigQuery table names representing nodes
- `edge_tables` -- an `ARRAY<STRING>` of BigQuery table names representing edges

> Note: you can leverage the fact the secret payload is JSON to tuck
> in any additional, supported arguments not exposed by your stored
> procedure. (For instance, you could override the default
> `neo4j_concurrency` setting.)

An example BigQuery SQL statement that calls the procedure:

```
DECLARE graph_name STRING DEFAULT "test-graph";
DECLARE graph_uri STRING DEFAULT "gs://your-bucket/folder/model.json";
DECLARE neo4j_secret STRING DEFAULT "projects/123456/secrets/neo4j-bigquery/versions/1";
DECLARE bq_project STRING DEFAULT "your-gcp-project";
DECLARE bq_dataset STRING DEFAULT "your_bq_dataset";
DECLARE node_tables ARRAY<STRING> DEFAULT ["papers", "authors", "institution"];
DECLARE edge_tables ARRAY<STRING> DEFAULT ["citations", "authorship", "affiliation"];

CALL `your-gcp-project.your_bq_dataset.neo4j_gds_graph_project`(
    graph_name, graph_uri, neo4j_secret, bq_project, bq_dataset,
    node_tables, edge_tables);
```

> Detailed documentation coming soon :-)

## Current Caveats

- All known caveats for populating GDS via Arrow Flight apply
  (e.g. node id formats, etc.).
- Concurrency doesn't auto-tune. Current recommendation is to set
  `neo4j_concurrency` to the number of AuraDS CPU / 2 at minimum, but
  it's not clear how much it helps.

## Copyright and Licensing

All artifacts and code in this project, unless noted in their
respective files, are copyright Neo4j and made available under the
Apache License, Version 2.0.

No support is currently provided.
