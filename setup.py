from setuptools import find_packages, setup

setup(
    name="dataproc-experiment",
    version="0.1.0",
    url="https://github.com/neo4j-field/dataproc-pyarrow-to-gds",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[
        "google-dataproc-templates == 0.0.3",
        "google-cloud-bigquery-storage[pyarrow] >= 2.18",
        "neo4j_arrow @ https://github.com/neo4j-field/neo4j_arrow/archive/refs/tags/0.4.0.tar.gz",
    ],
    packages=find_packages(),
)
