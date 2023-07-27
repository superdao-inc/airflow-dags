from typing import Optional

from airflow.models import Variable
from clickhouse_driver import Client


# Get clickhouse script
def get_ch_cli_script(
    query: str,
    settings: Optional[str] = '',
    logs_out: Optional[str] = '',
    script_prefix: Optional[str] = '',
) -> str:
    """
    Get ClickHouse CLI script
    :return: filled script
    """
    host = Variable.get('clickhouse_host')
    port = Variable.get('clickhouse_port')
    password = Variable.get('clickhouse_password')

    script = f'clickhouse-client --host {host} --port {port} --password {password} {settings} --query="{query}"'
    logs_output = f'&> {logs_out}' if logs_out else ''

    return f'{script_prefix} {script} {logs_output}'


# Execute query in clickhouse
def exec_ch_driver_query(query: str):
    """ "
    Execute query into Clickhouse use clickhouse_driver
    :return: output query
    """

    host = Variable.get('clickhouse_host')
    port = Variable.get('clickhouse_port')
    password = Variable.get('clickhouse_password')

    client = Client(
        host=host, port=port, password=password, database='default', user='default'
    )
    output = client.execute(query)

    return output
