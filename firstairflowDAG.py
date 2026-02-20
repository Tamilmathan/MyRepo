
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 17),
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule='@daily',
    catchup=False,
) as dag:
    # Define tasks using operators
    task_1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello"',
    )

    task_2 = BashOperator(
        task_id='print_world',
        bash_command='echo "World"',
    )

    # Set dependencies
    task_1 >> task_2 # task_2 runs after task_1
