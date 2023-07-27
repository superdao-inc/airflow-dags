import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook


# Execute query in clickhouse
def pg_driver_fetchone(query: str):
    """ "
    Execute query into Clickhouse use clickhouse_driver
    :return: output query
    """
    conn_param = BaseHook.get_connection('pg-prod-scoring-api')
    conn = psycopg2.connect(
        host=conn_param.host,
        port=conn_param.port,
        database=conn_param.schema,
        user=conn_param.login,
        password=conn_param.password,
    )
    db_cursor = conn.cursor()
    db_cursor.execute(query)
    res = db_cursor.fetchone()[0]

    return res


def pg_driver_execute(query: str):
    conn_param = BaseHook.get_connection('pg-prod-scoring-api')
    conn = psycopg2.connect(
        host=conn_param.host,
        port=conn_param.port,
        database=conn_param.schema,
        user=conn_param.login,
        password=conn_param.password,
    )
    db_cursor = conn.cursor()
    db_cursor.execute(query)


def load_pg_driver_into_pandas(query: str):
    conn_param = BaseHook.get_connection('pg-prod-scoring-api')

    with psycopg2.connect(
        "host='{}' port={} dbname='{}' user={} password={}".format(
            conn_param.host,
            conn_param.port,
            conn_param.schema,
            conn_param.login,
            conn_param.password,
        )
    ) as conn:
        res = pd.read_sql_query(query, conn)

    return res
