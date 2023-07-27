import json
import logging
import os
import subprocess
from tempfile import TemporaryDirectory

import pandas as pd
import requests

from common.chains.ethereum.ethereum_etl.functions import (
    get_etl_bucket_path,
    gzip_csv,
    unzip_csv,
)
from common.clickhouse import exec_ch_driver_query, get_ch_cli_script
from common.constants import TEMP_DIR
from common.gcs import download_from_gcs, upload_to_gcs


def export_snapshot_votes(**op_kwargs):
    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        output_dir = os.path.join(tempdir, f'votes.csv')
        logging.info(f"output_dir: {output_dir}")

        created_from = exec_ch_driver_query(
            f'SELECT max(created) FROM raw_data_snapshot.votes'
        )[0][0]
        created = created_from
        logging.info(f"created_from: {created_from}")
        data = pd.DataFrame()

        while True:
            query = (
                '''query Votes2 {
                        votes (
                            first: 1000,
                            skip: 0,
                            orderBy: "created",
                            orderDirection: asc,
                            where: { created_gte: '''
                + str(created)
                + '''}
                        ) {
                            id
                            voter
                            created
                            choice
                            space {
                            id
                            }
                        }
                        }'''
            )

            url = 'https://hub.snapshot.org/graphql/'
            try:
                r = requests.post(url, json={'query': query})
                votes_data = json.loads(r.text)['data']['votes']
                created = votes_data[-1]['created']

                for i in range(len(votes_data)):
                    votes_data[i]['space'] = votes_data[i]['space']['id']

                tmp = pd.DataFrame(votes_data)

                data = pd.concat([tmp, data])
                lens = len(tmp)
                print('real lens:', lens, '; timer: ', created)

                if lens < 2 or lens != 1000:
                    break

            except Exception as err:
                print('error ', err)
                print(query)

        created_to = created
        data.to_csv(output_dir, sep=',', index=False)

        gzipped_file_path = gzip_csv(csv_path=output_dir)

        bucket_object = get_etl_bucket_path(
            chain='snapshot',
            data_type='votes',
            start_block=created_from,
            end_block=created_to,
        )
        upload_to_gcs(
            object=bucket_object,
            filename=gzipped_file_path,
            bucket=f'ethereum-etl-data',
            content_type='application/x-gzip',
        )

        return {
            "start_block": created_from,
            "end_block": created_to,
            "bucket_object": bucket_object,
        }


def upload_data_to_clickhouse(**op_kwargs):
    ti = op_kwargs['ti']
    task_to_pull_from = op_kwargs['task_to_pull_from']

    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        exported_data = ti.xcom_pull(task_ids=task_to_pull_from)

        logging.info("Downloading file from GCS: %s", exported_data['bucket_object'])
        zip_file = os.path.join(tempdir, f'votes.csv.gz')

        download_from_gcs(
            bucket=f'ethereum-etl-data',
            object=exported_data['bucket_object'],
            filename=zip_file,
        )

        logging.info(f"Uploading to ClickHouse {exported_data}")
        csv_file = unzip_csv(csv_path=zip_file)

        clickhouse_upload_script = get_ch_cli_script(
            query=f"INSERT INTO raw_data_snapshot.votes FORMAT CSVWithNames",
            settings="--input_format_with_names_use_header 1",
            script_prefix=f'cat {csv_file} |',
        )

        subp = subprocess.run(clickhouse_upload_script, shell=True, capture_output=True)
        logging.info(f"subprocess returncode: {subp.returncode}. stdout: {subp.stdout}")
        logging.error(f"subprocess stderr: {subp.stderr}")

        return f"Done upload_to_clickhouse"
