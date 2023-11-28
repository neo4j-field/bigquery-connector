# Debian 11 is recommended.
FROM debian:11-slim AS base

# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini libjemalloc2 git curl && apt clean -y

# Enable jemalloc2 as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# (Optional) Install and configure Miniconda3.
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

# (Optional) Install Conda packages.
#
# The following packages are installed in the default image, it is strongly
# recommended to include all of them.
#
# Use mamba to install packages quickly.
#RUN ${CONDA_HOME}/bin/conda install mamba -n base -c conda-forge \
#    && ${CONDA_HOME}/bin/mamba install \
#      conda \
#      cython \
#      fastavro \
#      fastparquet \
#      gcsfs \
#      google-cloud-bigquery-storage \
#      google-cloud-bigquery[pandas] \
#      google-cloud-bigtable \
#      google-cloud-container \
#      google-cloud-datacatalog \
#      google-cloud-dataproc \
#      google-cloud-datastore \
#      google-cloud-language \
#      google-cloud-logging \
#      google-cloud-monitoring \
#      google-cloud-pubsub \
#      google-cloud-redis \
#      google-cloud-spanner \
#      google-cloud-speech \
#      google-cloud-storage \
#      google-cloud-texttospeech \
#      google-cloud-translate \
#      google-cloud-vision \
#      koalas \
#      matplotlib \
#      nltk \
#      numba \
#      numpy \
#      openblas \
#      orc \
#      pandas \
#      pyarrow \
#      pysal \
#      pytables \
#      python \
#      regex \
#      requests \
#      rtree \
#      scikit-image \
#      scikit-learn \
#      scipy \
#      seaborn \
#      sqlalchemy \
#      sympy \
#      virtualenv

FROM base AS builder

# Build Neo4j Connector for BigQuery
ENV PYTHONPATH=/opt/python/packages
WORKDIR /tmp/code
COPY . .
RUN mkdir -p "${PYTHONPATH}" \
      && pip install poetry==1.7.1 \
      && poetry config virtualenvs.create false \
      && poetry install --no-root \
      && poetry build \
      && poetry export --format=requirements.txt --output=/tmp/code/requirements.txt --only=main --without-hashes

FROM base

# Install
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
