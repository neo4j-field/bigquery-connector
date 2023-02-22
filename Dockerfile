### From https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers
### as of: Last updated 2023-02-22 UTC

# Debian 11 is recommended.
FROM debian:11-slim AS base

# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# (Required) Install utilities required by Spark scripts.
RUN apt-get update \
    && apt-get install -y procps tini libjemalloc2 \
    && apt-get install -y python3 python3-pip \
    && apt-get auto-remove -y

# Enable jemalloc2 as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

RUN pip install -U pip && \
    pip install \
        google-dataproc-templates==0.0.3 \
        google-cloud-bigquery-storage[pyarrow]>=2.18 \
        graphdatascience==1.6 \
        pandas \
        pyarrow>=4,<11
ENV PYSPARK_PYTHON=/usr/bin/python3

ENV PYTHONPATH=/opt/python/packages
RUN mkdir -p "${PYTHONPATH}"

# (Required) Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark


FROM base
ENV PYTHONPATH=/opt/python/packages
COPY templates "${PYTHONPATH}"/templates
