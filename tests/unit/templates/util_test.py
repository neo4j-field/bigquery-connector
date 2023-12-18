import os
from templates import BigQueryToNeo4jGDSTemplate, constants as c
from templates import util


def test_parsing_bq_params() -> None:
    """
    Make sure our BigQuery param extraction passes through our ArgumentParser
    producing expected output.
    """
    os.environ.update(
        {
            f"{util.BQ_PARAM_PREFIX}{c.NEO4J_USER}": '"Dave"',
            f"{util.BQ_PARAM_PREFIX}{c.NEO4J_GRAPH_JSON}": '{"name": "Dave"}',
            f"{util.BQ_PARAM_PREFIX}{c.NODE_TABLES}": '["papers", "authors"]',
        }
    )

    params = util.bq_params()
    args = BigQueryToNeo4jGDSTemplate.parse_args(params)

    # Easy string parsing
    assert args[c.NEO4J_USER] == "Dave"

    # Can we handle inlined JSON blobs?
    assert args[c.NEO4J_GRAPH_JSON] == '{"name": "Dave"}'

    # What about BigQuery sending us ARRAYs?
    assert args[c.NODE_TABLES] == ["papers", "authors"]
