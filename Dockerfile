FROM apache/airflow:2.10.4-python3.12

# Install against Airflow's official constraints so nothing Airflow depends on
# gets upgraded underneath it.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

COPY --chown=airflow:root pipeline /opt/airflow/pipeline
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow
