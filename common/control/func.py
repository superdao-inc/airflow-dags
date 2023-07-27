import json

import requests
from airflow.models import BaseOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.decorators import apply_defaults


class RunDagAPIOperator(BaseOperator):
    @apply_defaults
    def __init__(self, dag_run, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dag_run = dag_run
        self.api_endpoint = (
            f"https://airflow.superdao.dev/api/v1/dags/{dag_run}/dagRuns"
        )
        self.api_token = "Basic YXBpOlhDbkRMRWlGZTc1VTRlbFB3OHppMkdpZ3hZa0Q="

    def execute(self, context):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.api_token,
        }
        data = {"conf": {}, "logical_date": context['ts']}
        r = requests.post(self.api_endpoint, headers=headers, data=json.dumps(data))
        self.log.info(self.api_endpoint)
        self.log.info(headers)
        self.log.info(json.dumps(data))
        self.log.info(r.text)
        self.log.info("DAG successfully triggered via API")


class CheckDagStateOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, task_id, dag_id, external_dag_id, external_task_id, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_id = task_id
        self.dag_id = dag_id
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def execute(self, context):
        sensor_task = ExternalTaskSensor(
            task_id=f"{self.dag_id}.{self.task_id}",
            external_dag_id=self.external_dag_id,
            external_task_id=self.external_task_id,
            mode="reschedule",
            timeout=600,  # Timeout value in seconds
            retries=3,  # Number of retries
            poke_interval=30,  # Time interval between pokes in seconds
            dag=self.dag,
        )
        sensor_task.execute(context)
        self.log.info("External DAG finished successfully")
