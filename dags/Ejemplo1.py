from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    return 'Hello, World!'

def imprimir_calculo():
    a = 10
    b = 5
    return (a*b)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    dag_id='hello_world',
    default_args={'owner': 'airflow', 'depends_on_past': False, 'start_date': datetime(2024, 2, 10),'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=5),},
    description='A simple "Hello, World!" DAG',
    schedule_interval=timedelta(days=1),
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

calculo_task = PythonOperator(
    task_id='print_calculo',
    python_callable=imprimir_calculo,
    dag=dag,
)

hello_task>>calculo_task