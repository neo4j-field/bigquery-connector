from setuptools import find_packages, setup

setup(
    name="neo4j-bigquery-connector",
    version="0.2.0",
    url="https://github.com/neo4j-field/dataproc-templates",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "google-dataproc-templates >= 0.0.1",
        "google-cloud-bigquery-storage[pyarrow] >= 2.18",
        "neo4j_arrow @ https://github.com/neo4j-field/neo4j_arrow/archive/refs/tags/0.4.0.tar.gz",
        "graphdatascience == 1.6",
        "fsspec[gcs] >= 2023.1.0",
    ],
    packages=find_packages(),
)
