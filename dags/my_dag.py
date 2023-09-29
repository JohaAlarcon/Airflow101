from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_a():
    print('hi fron task a')

def print_b():
    print('hi fron task b')

with DAG ('my_dag', start_date=datetime(2022,1,1),
          description = 'A simple tutorial DAG', tags =['data_science'],
          schedule_interval='@daily', catchup=False) as dag:

    task_a = PythonOperator(task_id='task_a',python_callable=print_a )
    task_b = PythonOperator(task_id='task_b',python_callable=print_b )

    task_a >> task_b