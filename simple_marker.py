from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskMarker
from airflow.operators.bash_operator import BashOperator

from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['kybishop@backcountry.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'simple_marker',
    default_args=default_args,
    schedule_interval='45 8 * * *',
    catchup=True
)

simple_marker_task = ExternalTaskMarker(
    task_id='simple_marker_task',
    external_dag_id='simple_sensor',
    external_task_id='simple_sensor_task',
    execution_date='{{ (execution_date + macros.timedelta(minutes=15)).isoformat() }}',
    dag=dag
)

simple_bash_operator = BashOperator(
    task_id='simple_bash_operator_marker',
    bash_command='echo simple bash operator marker dag',
    dag=dag
)

simple_marker_task >> simple_bash_operator