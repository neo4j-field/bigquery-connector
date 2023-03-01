import os
import main
from . import BigQueryToNeo4jGDSTemplate, constants as c
from . import util

def test_bq_params() -> None:
    os.environ.update({f"{util.BQ_PARAM_PREFIX}neo4j_user": "Dave"})
    params = util.bq_params()
    args = BigQueryToNeo4jGDSTemplate.parse_args(params)
    assert args[c.NEO4J_USER] == "Dave"
