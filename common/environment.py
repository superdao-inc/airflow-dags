import os

ENVIRONMENT_TYPE = os.getenv('ENVIRONMENT_TYPE')

stands_schemas = {
    'clickhouse': {'prod': 'dbt', 'dev': 'dbt_dev', 'stage': 'dbt_stage'},
    'psql': {
        'prod': 'public',
        'dev': 'stg_airflow',
        'stage': 'public',
    },
}

CURRENT_SCHEMA_DBT = stands_schemas['clickhouse'][ENVIRONMENT_TYPE]
CURRENT_SCHEMA_SCORING_API = stands_schemas['psql'][ENVIRONMENT_TYPE]
