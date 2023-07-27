import logging
import os
import time
from datetime import timedelta
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import psycopg2
import requests
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from annoy import AnnoyIndex
from psycopg2.extras import DictCursor
from sklearn.preprocessing import StandardScaler

from common.dags_config import create_dag
from common.environment import CURRENT_SCHEMA_SCORING_API
from common.gcs import GCS_ETL_BUCKET, upload_to_gcs
from common.slack_notification import slack_fail_alert

default_args = {}

SQL = f"""--sql
    SELECT
        wallet as address,
        0,
        0,
        coalesce(nfts_count, 0),
        coalesce(wallet_usd_cap, 0),
        superrank
    FROM scoring_api.{CURRENT_SCHEMA_SCORING_API}.mv__wallet_attributes
    ORDER BY superrank DESC;
"""

dag = create_dag(
    dag_id="build_wallet_annoy_index",
    schedule_interval="0 5 * * *",
    description="Building annoy index for wallet search",
    catchup=False,
    start_date=days_ago(2),
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_fail_alert,
)


with dag:
    start_task_id = "start_task"
    end_task_id = "end_task"
    create_annoy_index_task_id = "create_annoy_index"
    trigger_gitlab_pipeline_task_id = "trigger_gitlab_pipeline"

    start_task = EmptyOperator(task_id=start_task_id)
    end_task = EmptyOperator(task_id=end_task_id)

    @task(task_id=create_annoy_index_task_id)
    def create_annoy_index():
        start = time.time()

        conn_param = BaseHook.get_connection("pg-prod-scoring-api")
        conn = psycopg2.connect(
            host=conn_param.host,
            port=conn_param.port,
            database=conn_param.schema,
            user=conn_param.login,
            password=conn_param.password,
        )
        cursor = conn.cursor("server_side_cursor", cursor_factory=DictCursor)
        cursor.itersize = 1_000_000

        counter = 0
        batch_counter = 0

        cursor.execute(SQL)

        addresses = []
        wallets_data = []
        for row in cursor:
            wallet_address = row[0]
            addresses.append(wallet_address)

            vector = row[1:]
            wallets_data.append(vector)

            counter += 1

            if counter % 1_000_000 == 0:
                batch_counter += 1
                logging.info("Processed {} rows".format(counter))

        # Z-transform
        scaler = StandardScaler()
        wallets_data_np = np.vstack(wallets_data)
        print(wallets_data_np[:10])
        wallets_data_np[:, 2] = np.log1p(wallets_data_np[:, 2])
        wallets_data_np[:, 3] = np.log1p(wallets_data_np[:, 3])
        print(wallets_data_np[:10])

        wallets_data_scaled = scaler.fit_transform(wallets_data_np)

        # Create annoy index.
        # Using euclidean distance, because in our current(May 2023)
        # use case it is more important to measure "Absolute differences",
        # rather than "Direction"(cosine similarity)
        n_dimensions = 5
        index = AnnoyIndex(f=n_dimensions, metric="euclidean")

        # Save wallets map
        with TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "wallets_map.csv")
            wallets_map = pd.DataFrame(columns=["wallet_index", "wallet_address"])
            wallets_map.to_csv(file_path, index=False)

            vector_weights = np.array([0, 0.1, 0.3, 0.3, 100])
            wallets_map_batch = []
            for i, wallet_vector in enumerate(wallets_data_scaled):
                wallet_vector = wallet_vector * vector_weights
                index.add_item(i, wallet_vector)

                wallet_address = addresses[i]
                wallets_map_batch.append(
                    {"wallet_index": i, "wallet_address": wallet_address}
                )

                if i % 1_000_000 == 0:
                    wallets_map = pd.DataFrame(wallets_map_batch)
                    wallets_map_batch = []
                    with open(file_path, "a") as f:
                        wallets_map.to_csv(f, header=False, index=False)
                    logging.info("Processed {} rows".format(i))

            logging.info("Uploading wallets_map to GCS")
            object_name = "ml/map.csv"
            upload_to_gcs(
                bucket=GCS_ETL_BUCKET,
                object=object_name,
                filename=file_path,
            )

        # Save annoy index
        with TemporaryDirectory() as tmp_dir:
            similar_wallets_path = os.path.join(tmp_dir, "similar_wallets.ann")

            logging.info("Building index")
            n_trees = 10
            n_jobs = -1
            index.build(n_trees=n_trees, n_jobs=n_jobs)

            logging.info("Index built")
            index.save(similar_wallets_path)

            logging.info(
                f"Uploading to GCS. File size is: {os.path.getsize(similar_wallets_path)}",
            )
            object_name = "ml/similar_wallets.ann"
            upload_to_gcs(
                bucket=GCS_ETL_BUCKET,
                object=object_name,
                filename=similar_wallets_path,
            )

        end = time.time()
        time_in_muinutes = (end - start) / 60
        logging.info(f"Time taken in minutes: {time_in_muinutes}")

    @task(task_id=trigger_gitlab_pipeline_task_id)
    def trigger_gitlab_pipeline():
        logging.info("Triggering gitlab pipeline")
        trigger_token = Variable.get("gitlab_similar_wallets_trigger_token")
        requests.post(
            "https://gitlab.superdao.co/api/v4/projects/94/trigger/pipeline",
            data={"token": trigger_token, "ref": "main"},
        )

    start_task >> create_annoy_index() >> trigger_gitlab_pipeline() >> end_task
