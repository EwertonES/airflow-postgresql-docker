# Imagem Base para criação da imagem com todas as dependencias

FROM apache/airflow:latest

RUN export AIRFLOW_HOME=~/airflow

USER root

RUN apt-get update && apt-get install -y

COPY requirements.txt ./

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt