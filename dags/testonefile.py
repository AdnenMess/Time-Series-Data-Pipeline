from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
from pathlib import Path


def files_names(**context):
    fs_conn = BaseHook.get_connection('fs_default')
    directory = Path(fs_conn.extra_dejson['path'])
    files = directory.glob('*.csv')
    file_list = list(files)
    print("Files Found:", file_list)
    return len(file_list) >= 2


with DAG(
    dag_id='test_file_sensor_dag',
    schedule_interval=None,
    start_date=datetime(2023, 5, 18),
) as dag:

    file_sensor_task = PythonSensor(
        task_id='file_sensor_task',
        python_callable=files_names,
        poke_interval=10,
    )
    # For single file we can use : file_sensor_task = FileSensor(task_id='file_sensor_task', poke_interval=10,
    # filepath="20160930_203718.csv.csv", fs_conn_id='fs_default',)

    dummy_task = DummyOperator(task_id='dummy_task')

    file_sensor_task >> dummy_task
