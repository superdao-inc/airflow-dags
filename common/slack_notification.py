import logging

import pytz
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = 'slack_notifications'


def convert_datetime(datetime_string):
    return datetime_string.astimezone(pytz.timezone('UTC')).strftime('%b-%d %H:%M:%S')


def _send_slack_notification(message, context):
    """
    Sends message to a Slack channel.
    If you want to send it to a "user" -> use "@user",
        if "public channel" -> use "#channel",
        if "private channel" -> use "channel"
    Usage:
        default_args = {
            'on_failure_callback': slack_fail_alert
        }
        dag = DAG(
            'dag_id',
            default_args=default_args,
            ...
        )
    """

    logging.info("Sending slack message", context)

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login

    log_url = context.get('task_instance').log_url
    log_url = log_url.replace('localhost:8080', 'airflow.superdao.dev')
    slack_msg = f"""
            {message}
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {convert_datetime(context.get('execution_date'))}
            <{log_url}|*Logs*>
        """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID,
    )

    return slack_alert.execute(context=context)


def slack_fail_alert(context):
    return _send_slack_notification(':x: Task Failed.', context)


def slack_success_notification(context):
    return _send_slack_notification(':white_check_mark: Task Succeeded.', context)
