# Add initial variables and connections for Airflow using airflow CLI inside docker container

# Examples:
# docker-compose -f deployment/local/docker-compose.yaml exec airflow-worker airflow variables set example_airflow_vriable EXAMPLE_AIRFLOW_VARIABLE_VALUE
# docker-compose -f deployment/local/docker-compose.yaml exec airflow-worker airflow connections add --conn-type CONN_TYPE --conn-host HOST --conn-schema SCHEMA --conn-login DB_LOGIN --conn-password DB_PASS --conn-port DB_PORT connection_name
