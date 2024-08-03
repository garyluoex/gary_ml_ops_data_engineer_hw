FROM apache/airflow:2.9.3-python3.12

# install required pypi dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# copy pipeline and data files
# COPY dags/ /opt/airflow/dags/
# COPY data/ /opt/airflow/data/

# initialize and start airflow
COPY scripts/entrypoint.sh /opt/airflow/scripts/entrypoint.sh
ENTRYPOINT [ "bash", "/opt/airflow/scripts/entrypoint.sh" ]
