# Neo4j BigQuery Connector

This project is a prototype of a Dataproc template to power a BigQuery
stored procedure for Apache Spark (aka Dataproc Serverless).

It allows for bidirectional data loading between Neo4j and BigQuery.

## Building

The code is packaged into a Docker image that gets deployed by
Dataproc onto the Spark environment.

To build:

```
$ docker build -t "eu.gcr.io/your-gcp-project/neo4j-bigquery-connector:0.4.0" .
```

Then push to Google Container Registry (GCR):

```
$ docker push "eu.gcr.io/your-gcp-project/neo4j-bigquery-connector:0.4.0"
```

> Note: you will need to enable your local gcloud tooling to help
> authenticate.  Try running: `gcloud auth configure-docker`

## Running

The template has been tested with AuraDS as well as self-managed Neo4j
v5 Enterprise.

### Network Prerequisites

In either case, you most likely need to configure a GCP network to use
[Private Google Access](https://cloud.google.com/vpc/docs/private-google-access)
and possibly Cloud NAT. (Cloud NAT is definitely needed for AuraDS.)

### Running Locally

If you want the fastest way to validate any changes, you can run the
PySpark jobs locally by running the entrypoint scripts.

First, install dependences into your virtual environment:

```
$ . venv/bin/activate  # assumes `venv` is your virtual env directory
(venv) $ pip install -r requirements.txt
(venv) $ pip install -r requirements-dev.txt
```

> Note: You may also need to install a Java 11 JRE and make sure
> `JAVA_HOME` is set.

Then invoke one of the `main*.py` entrypoint scripts using the command
line arguments supported by the template. For example:

```
(venv) $ python main_to_bq.py --graph_name=mag240 \
    --neo4j_secret="projects/1055617507124/secrets/neo4j-bigquery-demo-2/versions/2" \
    --bq_project=neo4j-se-team-201905 --bq_dataset=bqr_neo4j_demo --bq_table=results_nodes \
    --bq_sink_mode=nodes --neo4j_labels=Paper --neo4j_properties=flag,years
```

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
procedures, you need to create your stored procedure.


### Creating the BigQuery --> Neo4j Procedure:

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
    container_image='eu.gcr.io/your-gcp-project/neo4j-bigquery-connector:0.4.0',
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

### Creating the Neo4j --> BigQuery Procedures
One Dataproc template (`Neo4jGDSToBigQueryTemplate`) supports writing
Nodes or Relationships back to BigQuery from AuraDS/GDS. The mode is
simply toggled via a `--bq_sink_mode` parameter that can either be
hardcoded (like below) to make distinct stored procedures or exposed
as a parameter for the user to populate.

For Nodes:
```sql
CREATE OR REPLACE PROCEDURE
  `my-gcp-project.my_bigquery_dataset.neo4j_gds_stream_nodes`(
    graph_name STRING,
    neo4j_secret STRING,
    bq_project STRING,
    bq_dataset STRING,
    bq_table STRING,
    neo4j_labels ARRAY<STRING>,
    neo4j_properties ARRAY<STRING>)
WITH CONNECTION `your-gcp-project.eu.my-spark-connection` OPTIONS (
    engine='SPARK',
    runtime_version='2.0',
    container_image='eu.gcr.io/your-gcp-project/neo4j-bigquery-connector:0.4.0',
    properties=[("spark.executor.cores", "8"), ("spark.executor.memory", "16g")],
    description="Stream nodes from Neo4j AuraDS to BigQuery")
  LANGUAGE python AS R"""
# Need to back out this Spark BigQuery Support stuff.
import sys
print(f"original path: {sys.path}")
newpath = [p for p in sys.path if not "spark-bigquery-support" in p]
sys.path = newpath
print(f"new path: {sys.path}")

from pyspark.sql import SparkSession
from templates import Neo4jGDSToBigQueryTemplate

spark = (
	SparkSession
	.builder
	.appName("Neo4j -> BigQuery Connector")
	.getOrCreate()
)

template = Neo4jGDSToBigQueryTemplate()
hardcoded = [
    "--bq_sink_mode=nodes",
]
args = template.parse_args(hardcoded)
template.run(spark, args)
""";
```

For Relationships:
```sql
CREATE OR REPLACE PROCEDURE
  `my-gcp-project.my_bigquery_dataset.neo4j_gds_stream_edges`(
    graph_name STRING,
    neo4j_secret STRING,
    bq_project STRING,
    bq_dataset STRING,
    bq_table STRING,
    neo4j_types ARRAY<STRING>,
    neo4j_properties ARRAY<STRING>)
WITH CONNECTION `neo4j-se-team-201905.eu.voutila-bq-spark-eu` OPTIONS (engine='SPARK',
    runtime_version='2.0',
    container_image='eu.gcr.io/my-gcp-project/neo4j-bigquery-connector:0.4.0',
    properties=[
      ("spark.executor.cores", "8"),
      ("spark.executor.memory", "32g"),
      ("spark.dynamicAllocation.executorAllocationRatio", "1.0")
    ],
    description="Stream relationships from Neo4j AuraDS to BigQuery")
  LANGUAGE python AS R"""
# Need to back out this Spark BigQuery Support stuff.
import sys
print(f"original path: {sys.path}")
newpath = [p for p in sys.path if not "spark-bigquery-support" in p]
sys.path = newpath
print(f"new path: {sys.path}")

from pyspark.sql import SparkSession
from templates import Neo4jGDSToBigQueryTemplate

spark = (
	SparkSession
	.builder
	.appName("Neo4j -> BigQuery Connector")
	.getOrCreate()
)

template = Neo4jGDSToBigQueryTemplate()
hardcoded = [
    "--bq_sink_mode=edges",
]
args = template.parse_args(hardcoded)
print(f"args: {args}")
template.run(spark, args)
""";
```

Some details on the inputs:

- `graph_name` -- the name of the graph in AuraDS you're reading
- `neo4j_secret` -- a Google Secret Manager secret resource id
  containing a JSON blob with additional arguments:
  * `neo4j_user` -- name of the Neo4j user to connect as
  * `neo4j_password` -- password for the given user
  * `neo4j_host` -- hostname (_not Bolt uri_) of the AuraDS instance
- `bq_project` -- GCP project id owning the BigQuery source data
- `bq_dataset` -- BigQuery dataset name for the source data
- `bq_table` -- BigQuery name to write to
- `bq_sink_mode` -- either `"nodes"` or `"edges"`, to toggle the mode
- `neo4j_properties` -- an `ARRAY<STRING>` of property filters

Parameter(s) specific to `nodes` sink mode:
- `neo4j_labels` -- an `ARRAY<STRING>` of node label filters

Parameter(s) specific to `edges` sink mode:
- `neo4j_types` -- an `ARRAY<STRING>` of relationship type filters

> Note: Writing back to BigQuery is currently subject to
> preplanning your Spark environment to accomodate data sizes.
> See [Dataproc Serverless docs](https://cloud.google.com/dataproc-serverless/docs/concepts/properties#resource_allocation_properties)
> for options for increasing CPU or memory for Spark
> workers/executors.


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
