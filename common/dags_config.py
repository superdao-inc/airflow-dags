from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from airflow.models import DAG, Variable


def get_env_params() -> Dict:
    env_params = Variable.get('airflow_env_params', deserialize_json=True)
    env_mode = env_params['env_mode']

    output = {
        'env_mode': env_mode,
        'airflow_home_dir': env_params['airflow_home_dir'],
        'dbt_project_dir': env_params['dbt_project_dir'],
        'dbt_profiles_dir': env_params['dbt_profiles_dir'],
    }
    return output


def create_dag(
    dag_id: str,
    on_failure_callback: Optional[Callable] = None,
    schedule_interval: Optional[str] = None,
    description: Optional[str] = None,
    retries: int = 1,
    retry_delay: timedelta = timedelta(minutes=3),
    start_date: datetime = datetime(2023, 1, 1),
    execution_timeout: timedelta = timedelta(minutes=100),
    dagrun_timeout: timedelta = timedelta(minutes=100),
    sla: timedelta = timedelta(minutes=100),
    depends_on_past: bool = False,
    catchup: bool = False,
    tags: Optional[List[str]] = None,
    max_active_runs: int = 1,
    concurrency=None,
    pool: Optional[str] = None,
    # dbt_operator
    dbt_project_dir: Optional[str] = '/opt/dbt_clickhouse',
    dbt_profiles_dir: Optional[str] = '/opt/airflow/secrets',
    owner_links: Optional[dict] = None,
) -> DAG:
    # env params
    env_params = get_env_params()

    default_args = {
        'owner': 'superdao team',
        'sla': sla,
        'execution_timeout': execution_timeout,
        'depends_on_past': depends_on_past,
        'retries': retries,
        'retry_delay': retry_delay,
        'pool': pool,
        'dir': dbt_project_dir if dbt_project_dir else env_params['dbt_project_dir'],
        'profiles_dir': dbt_profiles_dir
        if dbt_profiles_dir
        else env_params['dbt_profiles_dir'],
    }

    return DAG(
        dag_id=dag_id,
        default_args=default_args,
        on_failure_callback=on_failure_callback,
        start_date=start_date,
        description=description,
        schedule_interval=schedule_interval,
        catchup=catchup,
        max_active_runs=max_active_runs,
        dagrun_timeout=dagrun_timeout,
        concurrency=concurrency,
        tags=tags,
        doc_md=__doc__,
        owner_links=owner_links,
    )
