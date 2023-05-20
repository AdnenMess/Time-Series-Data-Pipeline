from datetime import datetime
import psycopg2
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from transform import process_file, send_file_influxdb


def check_for_csv_file_task():
    file_sensor = FileSensor(
        task_id='check_for_csv_file',
        poke_interval=10,
        fs_conn_id='fs_default',
        filepath=str(Path.cwd().parent / 'Data_input' / '*.csv')
    )
    file_found = file_sensor.poke(None)
    return file_found


def check_if_none_exist_task(file_path):
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


# def process_file_task(file_path):
#     process = process_file(file_path)
#     return process
#
#
# def send_file_influxdb_task(processed_file):
#     send_file_influxdb(processed_file)


with DAG(dag_id='gas_dag', schedule_interval=None, start_date=datetime(2023, 1, 1)) as dag:
    check_for_csv_file_task = PythonOperator(task_id='check_for_csv_file', python_callable=check_for_csv_file_task)
    check_if_none_exist_in_database_task = PythonOperator(task_id='check_if_none_exist',
                                                          python_callable=check_if_none_exist_task)
    # process_file_task = PythonOperator(task_id='process_file', python_callable=process_file)
    # send_file_influxdb_task = PythonOperator(task_id='send_file_influxdb', python_callable=send_file_influxdb)

    check_for_csv_file_task >> check_if_none_exist_in_database_task
    # >> process_file_task >> send_file_influxdb_task
