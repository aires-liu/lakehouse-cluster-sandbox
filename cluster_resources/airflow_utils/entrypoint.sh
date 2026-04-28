#!/bin/bash
set -e

if [ "$AIRFLOW_ROLE" = "webserver" ]; then
  airflow db migrate

  # Only create the admin user if it does not already exist.
  if ! airflow users list | grep -q admin; then
    airflow users create \
      --username admin \
      --firstname admin \
      --lastname admin \
      --role Admin \
      --email admin@example.com \
      --password password
  fi

  # Always refresh variables and connection on startup.
  airflow variables set endpoint_url http://minio:9000
  airflow variables set aws_access_key_id admin
  airflow variables set aws_secret_access_key password

  airflow connections delete "spark_default" || true
  airflow connections add "spark_default" \
    --conn-type "spark" \
    --conn-host "spark://spark-master" \
    --conn-port "7077"

  airflow webserver
elif [ "$AIRFLOW_ROLE" = "scheduler" ]; then
  airflow db migrate || true
  airflow scheduler
fi
