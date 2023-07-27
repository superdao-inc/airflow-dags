import datetime
import json
import os

import clickhouse_connect
import requests
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google.json"

if requests is None:
    print("requests is None")

superdao_bucket = "superdao-etl-data"

chain_x_databases = [
    {
        "chain": "ethereum",
        "bq_dataset": "bigquery-public-data.crypto_ethereum",
        "ch_db": "eth_data",
    },
    {
        "chain": "polygon",
        "bq_dataset": "public-data-finance.crypto_polygon",
        "ch_db": "polygon_data",
    },
]

eth_table_x_bucket = [
    {
        "bq_table": "blocks",
        "ch_table": "blocks_by_number",
        "bucket_folder": "blocks",
        "eth_last_block_query": "select number from eth_data.blocks_by_number order by number desc limit 1",
        "field": "number",
    },
    {
        "bq_table": "contracts",
        "ch_table": "contracts_by_address",
        "bucket_folder": "contracts",
        "eth_last_block_query": "select block_number from eth_data.traces_by_block order by block_number desc limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "token_transfers",
        "ch_table": "token_transfers_by_token",
        "bucket_folder": "token_transfers",
        "eth_last_block_query": "select block_number from eth_data.token_transfers_by_token order by block_number desc limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "traces",
        "ch_table": "traces_by_block",
        "bucket_folder": "traces",
        "eth_last_block_query": "select block_number from eth_data.traces_by_block order by block_number desc limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "transactions",
        "ch_table": "transactions_by_block",
        "bucket_folder": "transactions_with_receipts",
        "eth_last_block_query": "select block_number from eth_data.transactions_by_block order by block_number desc limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "transactions",
        "ch_table": "receipts_by_block",
        "bucket_folder": "receipts",
        "eth_last_block_query": "select block_number from eth_data.receipts_by_block order by block_number desc limit 1",
        "field": "block_number",
    },
]

polygon_table_x_bucket = [
    # {
    #     'bq_table': 'blocks',
    #     'ch_table': 'blocks_by_number',
    #     'bucket_folder': 'blocks',
    #     'polygon_last_block_query': "select number from polygon_data.blocks_by_number order by number desc limit 1",
    #     'field': 'number'
    # },
    {
        "bq_table": "contracts",
        "ch_table": "contracts_by_address",
        "bucket_folder": "contracts",
        "polygon_last_block_query": "select 35 * 1000 * 1000 as block_number from polygon_data.transactions_by_block limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "transactions",
        "ch_table": "transactions_by_block",
        "bucket_folder": "transactions_with_receipts",
        "polygon_last_block_query": "select block_number from polygon_data.transactions_by_block order by block_number desc limit 1",
        "field": "block_number",
    },
    {
        "bq_table": "token_transfers",
        "ch_table": "token_transfers_by_token",
        "bucket_folder": "token_transfers",
        "polygon_last_block_query": "select block_number from polygon_data.token_transfers_by_token order by block_number desc limit 1",
        "field": "block_number",
    },
]


def get_query(db, bucket_fullname, field, last_block):
    return f"""
    EXPORT DATA OPTIONS (
        uri = 'gs://{bucket_fullname}',
        format = 'JSON',
        overwrite = true,
        compression = 'GZIP'
    )
    AS (
        SELECT *
        FROM `{db}`
        WHERE {field} > {last_block}
    );
    """


def get_custom_receipts_query(db, bucket_fullname, field, last_block):
    # block_number        Int256,
    # hash                String,
    # cumulative_gas_used Int256,
    # gas_used            Int256,
    # contract_address    String,
    # root                String,
    # status              Int64,
    # effective_gas_price Int256

    return f"""
    EXPORT DATA OPTIONS (
        uri = 'gs://{bucket_fullname}',
        format = 'JSON',
        overwrite = true,
        compression = 'GZIP'
    )
    AS (
        SELECT 
        block_number,
        tx.hash,
        receipt_cumulative_gas_used as cumulative_gas_used,
        receipt_gas_used as gas_used,
        receipt_contract_address as contract_address,
        receipt_root as root,
        receipt_status as status,
        receipt_effective_gas_price as effective_gas_price
        FROM `bigquery-public-data.crypto_ethereum.transactions` as tx
        WHERE block_number > {last_block}
    );
    """


def get_clickhouse_client():
    with open("clickhouse.json") as f:
        clickhouse_env = json.load(f)

    return clickhouse_connect.get_client(
        host=clickhouse_env.get("host"),
        port=clickhouse_env.get("port"),
        username=clickhouse_env.get("username"),
        password=clickhouse_env.get("password"),
    )


def get_ch_db_by_chain(chain):
    if chain == "ethereum":
        return "eth_data"
    elif chain == "polygon":
        return "polygon_data"


def run_airflow_dag(chain, prefix, table):
    # {
    #   "bucket": "superdao-etl-data",
    #   "database": "polygon_data",
    #   "delimiter": ".json.gzip",
    #   "format": "CSVWithNames",
    #   "prefix": "polygon/contracts/contracts_",
    #   "table": "contracts_by_address_with_erc1155"
    # }
    dag_id = "from_gcs_to_clickhouse"

    with open("airflow.json") as f:
        airflow_env = json.load(f)
        auth = airflow_env.get("auth")

    ch_db = get_ch_db_by_chain(chain)
    url = f"https://airflow.superdao.dev/api/v1/dags/{dag_id}/dagRuns"
    auth_header = auth

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": auth_header,
    }

    payload = {
        "conf": {
            "bucket": superdao_bucket,
            "database": ch_db,
            "delimiter": ".json.gzip",
            "format": "JSONEachRow",
            "prefix": prefix,
            "table": table,
        }
    }
    print("payload", payload)

    requests.post(url, headers=headers, data=json.dumps(payload))


def main():
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    bq_client = bigquery.Client()
    clickhouse_client = get_clickhouse_client()

    for ch_x_bq in chain_x_databases:
        chain = ch_x_bq.get("chain")
        bq_dataset = ch_x_bq.get("bq_dataset")

        if chain == "ethereum":
            if chain == "ethereum":
                print("skip ethereum")
                continue

            for tb in eth_table_x_bucket:
                bq_table = tb.get("bq_table")
                bigquery_db = f"{bq_dataset}.{bq_table}"
                print("bigquery_db", bigquery_db)

                bucket_folder = tb.get("bucket_folder")

                if bucket_folder != "blocks":
                    continue

                bucket_object = f"{bucket_folder}-{date}-*.json.gz"
                bucket_fullname = (
                    f"{superdao_bucket}/{chain}/{bucket_folder}/{bucket_object}"
                )
                print(bucket_fullname)

                eth_last_block_query = tb.get("eth_last_block_query")
                last_block_result = clickhouse_client.query(eth_last_block_query)

                # filter_field = 'number' if table == 'blocks' else 'block_number'(so dumb btw)
                filter_field = tb.get("field")
                last_block = last_block_result.first_item.get(filter_field)
                print("last_block", last_block)

                bigquery_query = get_query(
                    bigquery_db, bucket_fullname, filter_field, last_block
                )

                # TODO: updated receipts, move them to transaction table
                if bucket_folder == "receipts":
                    bigquery_query = get_custom_receipts_query(
                        bigquery_db, bucket_fullname, filter_field, last_block
                    )
                print("Running query", bigquery_query)
                bq_job = bq_client.query(bigquery_query)
                bq_job.result()

                prefix = (
                    f'{chain}/{bucket_folder}/{bucket_object.replace("*.json.gz", "")}'
                )
                print(prefix)
                ch_table = tb.get("ch_table")
                run_airflow_dag(chain, prefix, ch_table)
                print("_____________________________________________________________")
                print("_____________________________________________________________")

        elif chain == "polygon":
            for polygon_tb in polygon_table_x_bucket:
                bq_table = polygon_tb.get("bq_table")
                bigquery_db = f"{bq_dataset}.{bq_table}"
                print("bigquery_db", bigquery_db)

                bucket_folder = polygon_tb.get("bucket_folder")
                bucket_object = f"{bucket_folder}-{date}-*.json.gz"
                bucket_fullname = (
                    f"{superdao_bucket}/{chain}/{bucket_folder}/{bucket_object}"
                )
                print(bucket_fullname)

                polygon_last_block_query = polygon_tb.get("polygon_last_block_query")
                last_block_result = clickhouse_client.query(polygon_last_block_query)

                filter_field = polygon_tb.get(
                    "field"
                )  # filter_field = 'number' if table == 'blocks' else 'block_number'(so dumb btw)
                last_block = last_block_result.first_item.get(filter_field)
                print("last_block", last_block)

                bigquery_query = get_query(
                    bigquery_db, bucket_fullname, filter_field, last_block
                )
                print("Running query", bigquery_query)
                bq_job = bq_client.query(bigquery_query)
                bq_job.result()

                prefix = (
                    f'{chain}/{bucket_folder}/{bucket_object.replace("*.json.gz", "")}'
                )
                print(prefix)
                ch_table = polygon_tb.get("ch_table")
                run_airflow_dag(chain, prefix, ch_table)
                print("_____________________________________________________________")
                print("_____________________________________________________________")


main()

# print(get_query('bigquery-public-data.crypto_ethereum.blocks',
#                 'superdao-etl-data/ethereum/blocks/blocks-2021-09-01-*.json.gz', 'number', 13_000_000))
