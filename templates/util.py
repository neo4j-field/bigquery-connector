# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Utility functions.
"""
import os
import json

from typing import cast, Dict, List


BQ_PARAM_PREFIX = "BIGQUERY_PROC_PARAM."


def bq_params() -> List[str]:
    """
    BigQuery Stored Procedures for Apache Spark send inputs via the process's
    environment, encoding in JSON. Each one is prefixed with
    "BIGQUERY_PROC_PARAM.".

    Defers the JSON parsing to other functions that need to consume the values.
    """
    # This is an ugly typing dance :(
    env: Dict[str, str] = dict(os.environ)
    values: List[str] = []
    for k, v in env.items():
        if k.startswith(BQ_PARAM_PREFIX):
            values.append(f"--{k.replace(BQ_PARAM_PREFIX, '')}")

            # XXX TODO: properly handle the fact we get JSON data here
            val = json.loads(v)
            if isinstance(val, str):
                values.append(val)
            elif isinstance(val, list):
                # XXX for now, repack into a single string until arg parsing
                # supports native lists
                values.append(",".join(val))
            else:
                # Blind cast of the original. Yolo.
                values.append(str(v.strip()))
    return values


def fetch_secret(secret_id: str) -> Dict[str, str]:
    """
    Retrieve and decode a secret from Google Secret Manager.

    XXX We assume the operator has created a JSON payload containing valid
    parameters for our pipeline.
    """
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        request = secretmanager.AccessSecretVersionRequest(name=secret_id)
        secret = client.access_secret_version(request=request)
        payload = secret.payload.data.decode("utf8")

        data = json.loads(payload)
        if isinstance(data, dict):
            # XXX assume Dict[str, str] for now. Should parse more explicitly.
            return cast(Dict[str, str], data)
    except Exception as e:
        print(f"failed to get secret {secret_id}: {e}")
    return {}
