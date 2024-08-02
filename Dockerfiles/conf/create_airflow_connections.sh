#!/bin/bash

# create_airflow_connections.sh crea una conexión de Spark en Apache Airflow.

# Crear la conexión de Spark
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'