from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
    'simple_sensor',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=True
)

simple_sensor_task = ExternalTaskSensor(
    task_id='simple_sensor_task',
    external_dag_id='simple_marker',
    external_task_id='simple_marker_task',
    execution_delta=timedelta(minutes=15),
    mode='reschedule',
    timeout=(60 * 5),
    dag=dag
)

simple_bash_operator = BashOperator(
    task_id='simple_bash_operator_sensor',
    bash_command='echo simple bash operator sensor dag',
    dag=dag
)

simple_sensor_task >> simple_bash_operator