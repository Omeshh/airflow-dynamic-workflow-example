docker-compose -f /Users/opatil/repos/airflow/docker-compose-LocalExecutor-local.yml down
docker-compose -f /Users/opatil/repos/airflow/docker-compose-LocalExecutor-local.yml up -d
docker exec -it airflow_webserver_1 bash
