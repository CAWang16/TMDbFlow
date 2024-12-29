FROM apache/airflow:2.6.3

USER root
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    postgresql-client \
    tini \
    && apt-get clean

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Ensure airflow user exists
RUN adduser --system --disabled-login --ingroup root airflow || true

# Change ownership to airflow user
RUN chown -R airflow:root /opt/airflow

# Copy files into container
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins
COPY ./requirements.txt /opt/airflow/requirements.txt

# Switch to airflow user for running commands
USER airflow
RUN pip install --upgrade pip && pip install -r /opt/airflow/requirements.txt

ENTRYPOINT ["tini", "--", "airflow"]
