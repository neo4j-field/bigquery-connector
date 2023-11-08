from setuptools import find_packages, setup
from templates import __version__

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt", "r") as f:
    reqs = f.read().splitlines()

setup(
    name="neo4j-bigquery-connector",
    version=__version__,

    url="https://github.com/neo4j-field/bigquery-connector",
    author="Neo4j, Inc.",

    description="Google BigQuery connector for AuraDS & Neo4j with GDS",
    long_description=long_description,
    long_description_content_type="text/markdown",

    license="Apache License 2.0",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Typing :: Typed",
    ],
    python_requires=">=3.8",
    install_requires=reqs,
    packages=find_packages(),
)
