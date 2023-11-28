# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Utility functions.
"""
import logging
import os
import json
import typing

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


class SparkLogHandler(logging.Handler):
    def __init__(self, logger: typing.Any):
        super().__init__()
        if not logger:
            raise Exception("logger must be provided.")
        self.logger = logger

    def handle(self, record: logging.LogRecord) -> bool:
        msg = f"[{record.name}] {record.getMessage()}"

        if record.levelno == logging.CRITICAL:
            self.logger.fatal(msg)
        elif record.levelno == logging.FATAL:
            self.logger.fatal(msg)
        elif record.levelno == logging.ERROR:
            self.logger.error(msg)
        elif record.levelno == logging.WARNING:
            self.logger.warn(msg)
        elif record.levelno == logging.WARN:
            self.logger.warn(msg)
        elif record.levelno == logging.INFO:
            self.logger.info(msg)
        elif record.levelno == logging.DEBUG:
            self.logger.debug(msg)
        else:
            self.logger.info(msg)

        return True
