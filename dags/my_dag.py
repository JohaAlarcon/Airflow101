from airflow import DAG
from airflow.operators.python import PythonOperator
from include.helpers import print_msg
from datetime import datetime

with DAG ('my_dag', start_date=datetime(2022,1,1),
          schedule_interval='@daily', catchup=False) as dag:

    task = PythonOperator(
        task_id='task',
        python_callable=print_msg

    )