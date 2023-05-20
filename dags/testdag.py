from datetime import datetime
import psycopg2
from pathlib import Path
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import XCom
from transform import process_file, send_file_influxdb


def check_for_csv_file_task():
    # Fetch the connection details from Airflow's Connection object
    conn = BaseHook.get_connection('fs_default')
    host = conn.host

    # Build the correct filepath using the host and file pattern
    filepath = str(Path(host) / '*.csv')

    file_sensor = FileSensor(
        task_id='check_for_csv_file',
        poke_interval=10,
        fs_conn_id='fs_default',
        filepath=filepath
    )
    file_found = file_sensor.poke(None)

    # Store the file_found value in XCom to pass it to the next task
    if file_found:
        XCom.set(key='file_found', value=file_found)

    return file_found


def check_if_none_exist_task(**kwargs):
    # Extract file_found value from XCom
    file_found = kwargs['ti'].xcom_pull(task_ids='check_for_csv_file', key='file_found')

    # Return False if file_found is False or None
    if not file_found:
        return False

    # Extract file path from XCom
    file_path = kwargs['ti'].xcom_pull(task_ids='check_for_csv_file')

    # Extract file name from file path
    file_name = Path(file_path).name

    # Connect to PostgreSQL database
    conn = psycopg2.connect(user="airflow",
                            password="airflow",
                            host="postgres",
                            port="5432",
                            database="GasData")

    # Check if file name already exists in table
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gas_name WHERE file_name = %s", (file_name,))
    count = cur.fetchone()[0]
    # Return False if the file name already exists in table, otherwise return True
    if count > 0:
        cur.close()
        conn.close()
        return False
    # Close database connection
    cur.close()
    conn.close()
    return True


with DAG(dag_id='test_dag', schedule_interval=None, start_date=datetime(2023, 1, 1)) as dag:
    check_for_csv_file_task = PythonOperator(task_id='check_for_csv_file', python_callable=check_for_csv_file_task)
    check_if_none_exist_in_database_task = PythonOperator(task_id='check_if_none_exist',
                                                          python_callable=check_if_none_exist_task)

    check_for_csv_file_task >> check_if_none_exist_in_database_task
