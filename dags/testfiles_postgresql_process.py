from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from transform import process_file
from pathlib import Path
import psycopg2
import re


def files_names(**context):
    fs_conn = BaseHook.get_connection('fs_default')
    directory = Path(fs_conn.extra_dejson['path'])
    files = directory.glob('*.csv')
    files_list = [str(file) for file in files]  # Convert PosixPath objects to strings
    print("Files Found:", files_list)
    return files_list


def check_if_none_exist_task(files_path, **context):
    # Extract file name from file path
    file_name = Path(files_path).name

    # Connect to PostgreSQL database
    conn = psycopg2.connect(user="airflow",
                            password="airflow",
                            host="postgres",
                            port="5432",
                            database="GasData")

    # Check if file name already exists in table
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM gas_name WHERE file_name = %s""", (file_name,))
    count = cur.fetchone()[0]

    # Print the file name being checked
    print("Checking file:", file_name)

    if count > 0:
        cur.close()
        conn.close()
        print("File already exists in PostgreSQL:", file_name)
        return False

    # Insert the file name into the table
    try:
        cur.execute("""INSERT INTO gas_name (file_name) VALUES (%s)""", (file_name,))
        print("Inserted file name:", file_name)
        conn.commit()  # Commit the changes
    except Exception as e:
        print("An error occurred while inserting the file name:", str(e))
    finally:
        cur.close()
        conn.close()
        return True


with DAG(
        dag_id='test_files_postgresql_process',
        schedule_interval=None,
        start_date=datetime(2023, 5, 18),
        dagrun_timeout=timedelta(minutes=5),
) as dag:
    file_sensor_task = PythonOperator(
        task_id='file_sensor_task',
        python_callable=files_names,
        provide_context=True,
    )

    file_list = file_sensor_task.execute(context={})  # Execute file_sensor_task to get the file list

    for file_path in file_list:
        # Create a task ID for the check_existence_task by concatenating the string 'check_existence_task_' with the
        # result of the re.sub() function. The re.sub() function is replacing any character in the file_path variable
        # that is not a letter (a-z or A-Z), a number (0-9), a hyphen (-), an underscore (), or a period (.) with an
        # underscore (). This is done to ensure that the task ID only contains valid characters. For example,
        # if the file_path variable contains the value '20160930_203718.csv', then this line of code would create a
        # task ID of 'check_existence_task_20160930_203718_csv'
        task_id = re.sub(r'[^a-zA-Z0-9-_.]', '_', str(file_path))

        check_existence_task = PythonOperator(
            task_id='check_existence_task_' + task_id,
            python_callable=check_if_none_exist_task,
            op_kwargs={'files_path': str(file_path)},
            provide_context=True,
        )

        # Use the same task_id as check_existence_task with a suffix for process_file_task
        process_file_task = PythonOperator(
            task_id=task_id + '_process_file',
            python_callable=process_file,
            op_kwargs={'csv_file': str(file_path)},
            provide_context=True,
        )

        file_sensor_task >> check_existence_task >> process_file_task
