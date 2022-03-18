FROM apache/airflow:2.0.0-python3.8
RUN pip install --user psycopg2-binary
ENV AIRFLOW_HOME=/usr/local/airflow
COPY ./airflow.cfg /usr/local/airflow/airflow.cfg
