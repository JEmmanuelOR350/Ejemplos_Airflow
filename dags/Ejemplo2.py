from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print('¡Hola Mundo!')

def generate_random_numbers():
    num1 = random.randint(0, 50)
    num2 = random.randint(0, 50)
    resultado = num1+num2
    if (resultado%2 == 0):
        return 'print_resultado_par'
    else:
        return 'print_resultado_impar'

def imprimir_par():
    print("El resultado es par.")

def imprimir_impar():
    print("El resultado es impar")

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Crear un objeto DAG
dag = DAG(
    catchup=False,
    dag_id='ejemplo_suma',
    default_args=args,
    start_date=datetime(2024,1,1),
    description='DAG que genera dos números al azar y determina si la suma es par o impar.',
    schedule_interval='*/5 * * * *',
)

# Definir las tareas del DAG
task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

task2 = PythonOperator(
    task_id='generate_random_numbers',
    python_callable=generate_random_numbers,
    dag=dag
)

task3_par = PythonOperator(
    task_id='print_resultado_par',
    python_callable=imprimir_par,
    trigger_rule='none_failed',
    dag=dag
)

task3_impar = PythonOperator(
    task_id='print_resultado_impar',
    python_callable=imprimir_impar,
    trigger_rule='none_failed',
    dag=dag
)

task1 >> task2 >> [task3_par, task3_impar]