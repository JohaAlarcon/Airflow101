from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.helpers import chain

def print_a():
    print('hi fron task a')

def print_b():
    print('hi fron task b')

def print_c():
    print('hi fron task c')

def print_d():
    print('hi fron task d')

def print_e():
    print('hi fron task e')

with DAG ('my_dag', start_date=datetime(2022,1,1),
          description = 'A simple tutorial DAG', tags =['data_science'],
          schedule_interval='@daily', catchup=False) as dag:

    task_a = PythonOperator(task_id='task_a',python_callable=print_a )
    task_b = PythonOperator(task_id='task_b',python_callable=print_b )
    task_c = PythonOperator(task_id='task_c',python_callable=print_c )
    task_d = PythonOperator(task_id='task_d',python_callable=print_d )
    task_e = PythonOperator(task_id='task_e',python_callable=print_e )

    chain (task_a , [task_b , task_c],[task_d , task_e])