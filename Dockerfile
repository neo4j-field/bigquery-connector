# Debian 11 is recommended.
FROM debian:11-slim AS base

# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini libjemalloc2 git curl && apt clean -y

# Enable jemalloc2 as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Install Miniconda3.
ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
RUN curl -L --fail https://repo.anaconda.com/miniconda/Miniconda3-py39_23.10.0-1-Linux-$(uname -m).sh -o /tmp/miniconda.sh \
  && bash /tmp/miniconda.sh -b -p /opt/miniconda3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --prepend channels conda-forge \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict \
  && rm -rf /tmp/miniconda.sh

# Build Neo4j Connector for BigQuery
FROM base AS builder

ENV PYTHONPATH=/opt/python/packages
WORKDIR /tmp/code
COPY . .
RUN mkdir -p "${PYTHONPATH}" \
      && pip install poetry==1.7.1 \
      && poetry config virtualenvs.create false \
      && poetry install --no-root \
      && poetry build \
      && poetry export --format=requirements.txt --output=/tmp/code/requirements.txt --only=main --without-hashes

# Install
FROM base

ENV PYTHONPATH=/opt/python/packages
COPY --from=builder /tmp/code/requirements.txt /tmp/requirements.txt
COPY --from=builder /tmp/code/dist/*.whl /tmp/
RUN pip install -r /tmp/requirements.txt \
        && pip install --no-deps --target $PYTHONPATH /tmp/*.whl \
        && pip uninstall -y pyspark \
        && pip cache purge \
        && rm -rf /tmp/requirements.txt \
        && rm -rf /tmp/*.whl

# (Required) Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
