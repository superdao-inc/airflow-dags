import logging
import os
import subprocess
from tempfile import TemporaryDirectory

from airflow.models import Variable
from ethereumetl.cli import (
    export_blocks_and_transactions,
    export_receipts_and_logs,
    export_token_transfers,
    export_traces,
    extract_contracts,
    extract_csv_column,
)

from common.chains.ethereum.ethereum_etl.functions import (
    get_etl_bucket_path,
    gzip_csv,
    unzip_csv,
)
from common.clickhouse import exec_ch_driver_query, get_ch_cli_script
from common.constants import TEMP_DIR
from common.gcs import download_from_gcs, upload_to_gcs


def export_data(**op_kwargs):
    ti = op_kwargs['ti']
    table = op_kwargs['table']
    task_to_pull_from = op_kwargs['task_to_pull_from']
    blockchain = op_kwargs['blockchain']

    own_ethereum_provider_uri = {
        'ethereum': 'http://eth.superdao.dev/node/1',
        'polygon': 'http://polygon.superdao.dev/node/1',
        'optimism': 'http://optimism.superdao.dev/',
        'arbitrum': 'http://arbitrum.superdao.dev',
        'avalanche': 'http://avalanche.superdao.dev/ext/bc/C/rpc',
    }
    public_ethereum_provider_uri = {
        'ethereum': 'https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161',
        'polygon': 'https://polygon-rpc.com/',
        'optimism': 'http://optimism.superdao.dev/',
        'arbitrum': 'http://arbitrum.superdao.dev',
        'avalanche': 'http://avalanche.superdao.dev/ext/bc/C/rpc',
    }

    own_ethereum_provider_uri = own_ethereum_provider_uri[blockchain]
    public_ethereum_provider_uri = public_ethereum_provider_uri[blockchain]

    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        output_dir = os.path.join(tempdir, f'{table}.csv')
        logging.info(f"output_dir: {output_dir}")

        files = {
            'blocks': f'--blocks-output {output_dir}',
            'transactions': f'--transactions-output {output_dir}',
            'traces': f'--output {output_dir}',
            'receipts': f'--receipts-output {output_dir}',
            'logs': f'--logs-output {output_dir}',
            'contracts': f'--output {output_dir}',
            'token_transfers': f'--output {output_dir}',
        }

        export_file = files[table]
        [start_block, end_block] = ti.xcom_pull(task_ids=task_to_pull_from)
        batch_size = 100

        if table in ['blocks', 'transactions']:
            subp_res = subprocess.run(
                f'ethereumetl export_blocks_and_transactions --start-block {start_block} --end-block {end_block} {export_file} --provider-uri {public_ethereum_provider_uri} --batch-size {batch_size} --max-workers 5',
                shell=True,
                capture_output=True,
            )

            logging.info(
                f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}"
            )
            logging.error(f"subprocess stderr: {subp_res.stderr}")
            logging.info(f"subprocess: {subp_res}")

        elif table == 'traces':
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                provider_uri=own_ethereum_provider_uri,
                batch_size=batch_size,
                output=output_dir,
                max_workers=5,
                genesis_traces=True,
                daofork_traces=True,
            )

        elif table in ['receipts', 'logs']:
            transactions_file = os.path.join(tempdir, f'transactions.csv.gz')
            transactions_object = str(
                get_etl_bucket_path(
                    chain=blockchain,
                    data_type='transactions',
                    start_block=start_block,
                    end_block=end_block,
                )
            )
            download_from_gcs(
                bucket=f'ethereum-etl-data',
                object=transactions_object,
                filename=transactions_file,
            )
            csv_file = unzip_csv(csv_path=transactions_file)

            # Step 1
            transaction_hashes_file = os.path.join(tempdir, f'transaction_hashes.txt')
            subp_res = subprocess.run(
                f'ethereumetl extract_csv_column --input {csv_file} --column hash --output {transaction_hashes_file}',
                shell=True,
                capture_output=True,
            )
            logging.info(
                f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}"
            )
            logging.error(f"subprocess stderr: {subp_res.stderr}")

            # Step 2
            subp_res = subprocess.run(
                f'ethereumetl export_receipts_and_logs --transaction-hashes {transaction_hashes_file} {export_file} --provider-uri {public_ethereum_provider_uri} --batch-size {batch_size} --max-workers 5',
                shell=True,
                capture_output=True,
            )
            logging.info(
                f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}"
            )
            logging.error(f"subprocess stderr: {subp_res.stderr}")
            logging.info(f"subprocess: {subp_res}")

        elif table == 'contracts':
            traces_file = os.path.join(tempdir, f'traces.csv.gz')
            traces_object = str(
                get_etl_bucket_path(
                    chain=blockchain,
                    data_type='traces',
                    start_block=start_block,
                    end_block=end_block,
                )
            )
            download_from_gcs(
                bucket=f'ethereum-etl-data', object=traces_object, filename=traces_file
            )
            csv_file = unzip_csv(csv_path=traces_file)

            # Step 1
            extract_contracts.callback(
                traces=csv_file, batch_size=batch_size, output=output_dir, max_workers=5
            )

        elif table == 'token_transfers':
            logs_file = os.path.join(tempdir, f'logs.csv.gz')
            logs_object = str(
                get_etl_bucket_path(
                    chain=blockchain,
                    data_type='logs',
                    start_block=start_block,
                    end_block=end_block,
                )
            )
            download_from_gcs(
                bucket=f'ethereum-etl-data', object=logs_object, filename=logs_file
            )
            csv_file = unzip_csv(csv_path=logs_file)
            subp_res = subprocess.run(
                f'ethereumetl extract_token_transfers --logs {csv_file} {export_file}',
                shell=True,
                capture_output=True,
            )

            logging.info(
                f"subprocess returncode: {subp_res.returncode}. stdout: {subp_res.stdout}"
            )
            logging.error(f"subprocess stderr: {subp_res.stderr}")
            logging.info(f"subprocess: {subp_res}")

        gzipped_blocks_csv_file_path = gzip_csv(csv_path=output_dir)

        bucket_object = get_etl_bucket_path(
            chain=blockchain,
            data_type=table,
            start_block=start_block,
            end_block=end_block,
        )
        upload_to_gcs(
            object=bucket_object,
            filename=gzipped_blocks_csv_file_path,
            bucket=f'ethereum-etl-data',
            content_type='application/x-gzip',
        )

        return {
            "start_block": start_block,
            "end_block": end_block,
            "bucket_object": bucket_object,
        }


def upload_data_to_clickhouse(**op_kwargs):
    ti = op_kwargs['ti']
    table = op_kwargs['table']
    task_to_pull_from = op_kwargs['task_to_pull_from']
    blockchain = op_kwargs['blockchain']

    order_by = {
        'blocks': f'number',
        'transactions': f'block_number',
        'traces': f'block_number',
        'receipts': f'block_number',
        'logs': f'block_number',
        'contracts': f'block_number',
        'token_transfers': f'block_number',
    }

    order_by = order_by[table]

    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        exported_data = ti.xcom_pull(task_ids=task_to_pull_from)

        logging.info("Downloading file from GCS: %s", exported_data['bucket_object'])
        zip_file = os.path.join(tempdir, f'{table}.csv.gz')

        download_from_gcs(
            bucket=f'ethereum-etl-data',
            object=exported_data['bucket_object'],
            filename=zip_file,
        )

        logging.info(f"Uploading {table} to ClickHouse {exported_data}")
        csv_file = unzip_csv(csv_path=zip_file)

        drop_if_exists_buf_sql = f'''TRUNCATE TABLE IF EXISTS raw_data_{blockchain}.buf_{blockchain}_etl_{table}'''
        drop_buf = exec_ch_driver_query(drop_if_exists_buf_sql)
        logging.info(
            f"Truncate buf if exists raw_data_{blockchain}.buf_{blockchain}_etl_{table} {drop_buf}"
        )

        create_buf_sql = f'''CREATE TABLE IF NOT EXISTS raw_data_{blockchain}.buf_{blockchain}_etl_{table} ENGINE=MergeTree() ORDER BY {order_by} as SELECT * FROM raw_data_{blockchain}.{blockchain}_etl_{table} LIMIT 0'''
        create_buf = exec_ch_driver_query(create_buf_sql)
        logging.info(
            f"Create buf raw_data_{blockchain}.buf_{blockchain}_etl_{table} {create_buf}"
        )

        clickhouse_upload_script = get_ch_cli_script(
            query=f"INSERT INTO raw_data_{blockchain}.buf_{blockchain}_etl_{table} FORMAT CSVWithNames",
            settings="--input_format_with_names_use_header 1",
            script_prefix=f'cat {csv_file} |',
        )

        subp = subprocess.run(clickhouse_upload_script, shell=True, capture_output=True)
        logging.info(f"subprocess returncode: {subp.returncode}. stdout: {subp.stdout}")
        logging.error(f"subprocess stderr: {subp.stderr}")

        return f"Done upload_{table}_to_clickhouse"


def replace_data_to_raw(**op_kwargs):
    ti = op_kwargs['ti']
    table = op_kwargs['table']
    blockchain = op_kwargs['blockchain']

    order_by = {
        'blocks': f'number',
        'transactions': f'block_number',
        'traces': f'block_number',
        'receipts': f'block_number',
        'logs': f'block_number',
        'contracts': f'block_number',
        'token_transfers': f'block_number',
    }
    order_by = order_by[table]

    create_buf_sql = f'''INSERT INTO raw_data_{blockchain}.{blockchain}_etl_{table} 
                        SELECT * FROM raw_data_{blockchain}.buf_{blockchain}_etl_{table}
                        WHERE {order_by} > (SELECT max({order_by}) FROM raw_data_{blockchain}.{blockchain}_etl_{table})'''

    create_buf = exec_ch_driver_query(create_buf_sql)
    logging.info(
        f"Replace buf into raw_data_{blockchain}.{blockchain}_etl_{table} {create_buf}"
    )
