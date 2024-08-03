#!/bin/bash

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname Admin \
    --role Admin \
    --email airflow@airflow.airflow

airflow webserver --port 8080 &

airflow scheduler
