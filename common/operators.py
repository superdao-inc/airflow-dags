import csv
import io
import logging
import os
from contextlib import closing
from typing import *
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from psycopg2.extensions import AsIs, ISQLQuote, register_adapter
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql.dml import insert
from sqlalchemy.sql.expression import alias

GCS_TO_PG_METHOD_COPY_FROM_STDIN = 'copy_from_stdin'
GCS_TO_PG_METHOD_MULTI = 'multi'
GCS_TO_PG_METHOD_INSERT_ON_CONFLICT_UPDATE = 'insert_on_conflict_update'


class NdArray(ISQLQuote):
    def __init__(self, array):
        self.array = array

    def __conform__(self, protocol):
        if protocol == ISQLQuote:
            return self

    def getquoted(self):
        comma_separated = ','.join(str(x) for x in self.array)
        return AsIs(f"'{{{comma_separated}}}'").getquoted()


register_adapter(np.ndarray, NdArray)


class PostgresToGCSOperator(BaseOperator):
    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        postgres_conn_id: str,
        postgres_database: str,
        gcp_conn_id: str,
        bucket_name: str,
        object_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._sql = sql
        self._postgres_conn_id = postgres_conn_id
        self._postgres_database = postgres_database

        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._object_name = object_name

    def _execute_pg_hook(self) -> pd.DataFrame:
        pg_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id,
            database=self._postgres_database,
        )

        return pg_hook.get_pandas_df(self._sql)

    def _execute_gcs_hook(self, data: bytes) -> None:
        gcs_hook = GCSHook(gcp_conn_id=self._gcp_conn_id)

        gcs_hook.upload(
            bucket_name=self._bucket_name,
            object_name=self._object_name,
            data=data,
        )

    def _transform_pg_data(self, pg_df: pd.DataFrame) -> bytes:
        buff = io.BytesIO()
        with closing(buff):
            pg_df.to_parquet(buff, index=False)
            return buff.getvalue()

    def execute(self, context: Dict[str, Any]) -> str:
        pg_df = self._execute_pg_hook()
        if pg_df.empty:
            raise AirflowException("No data returned from Postgres query.")

        data = self._transform_pg_data(pg_df)

        self._execute_gcs_hook(data)

        return self._object_name


class ClickhouseToGCSOperator(BaseOperator):
    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        clickhouse_conn_id: str,
        clickhouse_database: str,
        gcp_conn_id: str,
        bucket_name: str,
        object_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self._sql = sql
        self._clickhouse_conn_id = clickhouse_conn_id
        self._clickhouse_database = clickhouse_database

        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._object_name = object_name

    def _get_ch_dataframe(self) -> pd.DataFrame:
        logging.info('querying clickhouse...')
        ch_hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._clickhouse_database,
        )

        return ch_hook.get_pandas_df(self._sql)

    def _execute_gcs_hook(self, data: bytes) -> None:
        logging.info('uploading data to gcs...')
        gcs_hook = GCSHook(gcp_conn_id=self._gcp_conn_id)

        gcs_hook.upload(
            bucket_name=self._bucket_name,
            object_name=self._object_name,
            data=data,
            chunk_size=1024 * 1024 * 10,
        )

    def _transform_ch_df(self, ch_df: pd.DataFrame) -> bytes:
        logging.info('transforming data...')
        buff = io.BytesIO()
        with closing(buff):
            ch_df.to_parquet(buff, index=False)
            return buff.getvalue()

    def execute(self, context: Dict[str, Any]) -> str:
        ch_df = self._get_ch_dataframe()
        if ch_df.empty:
            raise AirflowException("No data returned from ClickHouse query.")

        data = self._transform_ch_df(ch_df)

        self._execute_gcs_hook(data)

        return self._object_name


class GCSToClickhouseOperator(BaseOperator):
    def __init__(
        self,
        clickhouse_conn_id: str,
        clickhouse_database: str,
        clickhouse_table: str,
        gcp_conn_id: str,
        bucket_name: str,
        upload_task_id: str,
        columns_mapper: Dict[str, str],
        truncate_table: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self._clickhouse_conn_id = clickhouse_conn_id
        self._clickhouse_database = clickhouse_database
        self._clickhouse_table = clickhouse_table
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._upload_task_id = upload_task_id
        self._columns_mapper = columns_mapper
        self._truncate_table = truncate_table

    def _transform_gcs_data(self, data: bytes) -> pd.DataFrame:
        buff = io.BytesIO(data)
        with closing(buff):
            df = pd.read_parquet(buff)

        return df.rename(columns=self._columns_mapper)

    def _execute_gcs_hook(self, object_name) -> bytes:
        gcs_hook = GCSHook(gcp_conn_id=self._gcp_conn_id)

        return gcs_hook.download(
            bucket_name=self._bucket_name,
            object_name=object_name,
        )

    def _get_table(self) -> str:
        return f'{self._clickhouse_database}.{self._clickhouse_table}'

    def _execute_clickhouse_hook(self, df: pd.DataFrame) -> None:
        ch_hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._clickhouse_database,
        )
        client = ch_hook.get_conn()

        if self._truncate_table:
            client.execute(f'TRUNCATE TABLE {self._get_table()}')

        client.execute(f'INSERT INTO {self._get_table()} VALUES', df.to_dict('records'))

    def execute(self, context: Dict[str, Any]) -> Any:
        ti = context['task_instance']
        object_name = ti.xcom_pull(task_ids=self._upload_task_id)

        data = self._execute_gcs_hook(object_name)
        df = self._transform_gcs_data(data)
        return self._execute_clickhouse_hook(df)


class GCSToPostgresOperator(BaseOperator):
    def __init__(
        self,
        postgres_conn_id: str,
        postgres_database: str,
        postgres_table: str,
        gcp_conn_id: str,
        bucket_name: str,
        upload_task_id: str,
        chunk_size: Optional[int] = None,
        truncate: bool = True,
        if_exists: str = 'append',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_conn_id
        self._postgres_database = postgres_database
        self._postgres_table = postgres_table
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._upload_task_id = upload_task_id
        self._chunk_size = chunk_size
        self._truncate = truncate
        self._if_exists = if_exists

    def _transform_gcs_data(self, data: bytes) -> pd.DataFrame:
        buff = io.BytesIO(data)
        with closing(buff):
            return pd.read_parquet(buff)

    def _execute_gcs_hook(self, object_name) -> bytes:
        gcs_hook = GCSHook(gcp_conn_id=self._gcp_conn_id)

        return gcs_hook.download(
            bucket_name=self._bucket_name,
            object_name=object_name,
        )

    def _execute_pg_hook(self, df: pd.DataFrame) -> None:
        pg_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id,
            database=self._postgres_database,
        )

        if df.empty:
            raise AirflowException("No data returned from ClickHouse query.")

        engine = create_engine(pg_hook.get_uri())
        with engine.connect() as connection:
            with connection.begin():
                if self._truncate:
                    print(f"TRUNCATE TABLE {self._postgres_table}")
                    connection.execute("TRUNCATE TABLE {}".format(self._postgres_table))

                # TODO: remove this after fixing the issue with duplicate wallets
                if 'wallet' in df.columns:
                    df.drop_duplicates('wallet', inplace=True)

                df.to_sql(
                    self._postgres_table,
                    connection,
                    if_exists=self._if_exists,
                    index=False,
                    chunksize=self._chunk_size,
                    method='multi',
                )

    def execute(self, context: Dict[str, Any]) -> Any:
        ti = context['task_instance']
        object_name = ti.xcom_pull(task_ids=self._upload_task_id)

        data = self._execute_gcs_hook(object_name)
        pg_df = self._transform_gcs_data(data)
        return self._execute_pg_hook(pg_df)


class GCSToPostgresChunkedOperator(BaseOperator):
    """
    Reloads parquet dataset from GCS to PG using temporary file to avoid loading entire dataset in memory.
    """

    def __init__(
        self,
        postgres_conn_id: str,
        postgres_database: str,
        postgres_table: str,
        gcp_conn_id: str,
        bucket_name: str,
        upload_task_id: str,
        chunk_size: int,
        truncate: bool = True,
        if_exists: str = 'append',
        method: str = GCS_TO_PG_METHOD_MULTI,
        on_conflict_index_columns: Optional[List[str]] = None,
        postgres_schema: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_conn_id
        self._postgres_database = postgres_database
        self._postgres_table = postgres_table
        self._postgres_schema = postgres_schema
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._upload_task_id = upload_task_id
        self._chunk_size = chunk_size
        self._truncate = truncate
        self._if_exists = if_exists
        self._method = method
        self._on_conflict_index_columns = on_conflict_index_columns

    def _execute_gcs_hook(self, object_name: str) -> bytes:
        gcs_hook = GCSHook(gcp_conn_id=self._gcp_conn_id)
        print(f'START DOWNLOADING DATA FRAME {object_name}')

        return gcs_hook.download(
            bucket_name=self._bucket_name,
            object_name=object_name,
            chunk_size=self._chunk_size,
            filename='{}.parquet'.format(object_name.replace('/', '_')),
        )

    def _load_to_pg(self, df_path: str) -> None:
        pg_batch_size = self._chunk_size
        parquet_file = pq.ParquetFile(df_path)
        pg_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id,
            database=self._postgres_database,
        )

        print(
            f'START LOADING DATA TO POSTGRES, data frame path: {df_path}, chunk size: {pg_batch_size}'
        )

        def psql_insert_with_on_conflict(pd_table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = (
                insert(pd_table.table)
                .values(data)
                .on_conflict_do_update(
                    index_elements=self._on_conflict_index_columns,
                    set_=alias(pd_table.table, 'excluded').columns,
                )
            )

            result = conn.execute(stmt)
            return result.rowcount

        def psql_copy_from_stdin(table, conn, keys, data_iter: Iterable[Any]):
            dbapi_conn = conn.connection
            with dbapi_conn.cursor() as cur:
                s_buf = io.StringIO()
                writer = csv.writer(s_buf)
                writer.writerows(data_iter)
                s_buf.seek(0)

                columns = ','.join(['"{}"'.format(k) for k in keys])
                if table.schema:
                    table_name = '{}.{}'.format(table.schema, table.name)
                else:
                    table_name = table.name

                sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
                cur.copy_expert(sql=sql, file=s_buf)

        method = GCS_TO_PG_METHOD_MULTI
        if self._method == GCS_TO_PG_METHOD_INSERT_ON_CONFLICT_UPDATE:
            method = psql_insert_with_on_conflict
            print('USE PG REPLICATION METHOD: insert_on_conflict_update')
        elif self._method == GCS_TO_PG_METHOD_COPY_FROM_STDIN:
            method = psql_copy_from_stdin
            print('USE PG REPLICATION METHOD: copy_from_stdin')
        else:
            print('USE PG REPLICATION METHOD: multi (default)')

        engine = create_engine(pg_hook.get_uri())
        with engine.connect() as connection:
            with connection.begin():
                if self._truncate:
                    if self._postgres_schema is None:
                        table_with_schema = self._postgres_table
                    else:
                        table_with_schema = (
                            self._postgres_schema + '.' + self._postgres_table
                        )

                    print(f'TRUNCATE TABLE {table_with_schema}')
                    connection.execute("TRUNCATE TABLE {}".format(table_with_schema))

                for i, batch in enumerate(
                    parquet_file.iter_batches(batch_size=pg_batch_size)
                ):
                    batch_df = batch.to_pandas()

                    # translate array fields to pg array string
                    batch_df = batch_df.convert_dtypes(infer_objects=True)
                    series_columns = batch_df.select_dtypes(include=[pd.Series]).columns
                    if series_columns.values.size > 0:
                        print('SERIES COLUMNS FOUND', series_columns.values)
                        for col in series_columns:
                            batch_df[col] = batch_df[col].apply(
                                lambda x: (
                                    "{{{}}}".format(','.join(x))
                                    if hasattr(x, '__iter__')
                                    else x
                                )
                            )

                    batch_df.to_sql(
                        self._postgres_table,
                        connection,
                        if_exists='append',
                        index=False,
                        chunksize=self._chunk_size,
                        method=method,
                        schema=self._postgres_schema,
                    )
                    print(
                        'BATCH #{} LOADED, {} rows proceed'.format(
                            i + 1, (i + 1) * self._chunk_size
                        )
                    )

    def execute(self, context: Dict[str, Any]) -> Any:
        ti = context['task_instance']
        object_name = ti.xcom_pull(task_ids=self._upload_task_id)
        file_name = self._execute_gcs_hook(object_name)

        result = self._load_to_pg(file_name)

        os.remove(file_name)

        return result


class ClickhouseToS3Operator(BaseOperator):
    sql_template = """
INSERT INTO
FUNCTION s3('{endpoint}/{bucket_name}/{object_name}', '{access_key_id}', '{secret_key}', '{format}')
SELECT *
FROM {database}.{table}
"""

    def __init__(
        self,
        *,
        clickhouse_conn_id: str,
        clickhouse_database: str,
        clickhouse_table: str,
        s3_access_key_id: str,
        s3_secret_key: str,
        s3_endpoint: str,
        s3_bucket_name: str,
        object_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._clickhouse_conn_id = clickhouse_conn_id
        self._clickhouse_database = clickhouse_database
        self._clickhouse_table = clickhouse_table

        self._s3_access_key_id = s3_access_key_id
        self._s3_secret_key = s3_secret_key
        self._s3_endpoint = s3_endpoint
        self._s3_bucket_name = s3_bucket_name
        self._object_name = object_name

    def execute(self, context: Dict[str, Any]) -> str:
        ch_hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._clickhouse_database,
        )

        sql = self.sql_template.format(
            endpoint=self._s3_endpoint,
            bucket_name=self._s3_bucket_name,
            object_name=self._object_name,
            access_key_id=self._s3_access_key_id,
            secret_key=self._s3_secret_key,
            format='Parquet',
            database=self._clickhouse_database,
            table=self._clickhouse_table,
        )

        ch_hook.run(sql)

        return self._object_name


class S3ToClickhouseOperator(BaseOperator):
    sql_template = """
INSERT INTO {database}.{table}
SELECT * FROM
s3('{endpoint}/{bucket_name}/{object_name}', '{access_key_id}', '{secret_key}', '{format}')
"""

    template_fields: Sequence[str] = (
        "clickhouse_database",
        "clickhouse_table",
        "s3_bucket_name",
    )

    def __init__(
        self,
        *,
        clickhouse_conn_id: str,
        clickhouse_database: str,
        clickhouse_table: str,
        gcp_conn_id: str,
        s3_access_key_id: str,
        s3_secret_key: str,
        s3_endpoint: str,
        s3_bucket_name: str,
        s3_object_name: str,
        format: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.clickhouse_database = clickhouse_database
        self.clickhouse_table = clickhouse_table

        self.gcp_conn_id = gcp_conn_id

        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint = s3_endpoint
        self.s3_bucket_name = s3_bucket_name
        self.object_name = s3_object_name
        self.format = format

    def execute(self, context: Dict[str, Any]) -> str:
        ch_hook = ClickHouseHook(
            clickhouse_conn_id=self.clickhouse_conn_id,
            database=self.clickhouse_database,
        )

        sql = self.sql_template.format(
            endpoint=self.s3_endpoint,
            bucket_name=self.s3_bucket_name,
            object_name=self.object_name,
            access_key_id=self.s3_access_key_id,
            secret_key=self.s3_secret_key,
            format=self.format,
            database=self.clickhouse_database,
            table=self.clickhouse_table,
        )
        ch_hook.run(sql)
