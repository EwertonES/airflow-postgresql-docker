#!/usr/bin/env bash
export AIRFLOW_HOME=/opt/airflow

airflow db init
airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email airflow@example.com

airflow webserver