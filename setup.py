from setuptools import find_packages, setup

reqs = []
with open("requirements.txt", "r") as f:
    reqs.append(f.readline().strip())

setup(
    name="dataproc-experiment",
    version="0.0.1",
    url="https://github.com/neo4j-field/...",
    maintainer="Dave Voutila",
    maintainer_email="dave.voutila@neotechnology.com",
    license="Apache License 2.0",

    install_requires=[reqs],
    packages=find_packages(),
)
