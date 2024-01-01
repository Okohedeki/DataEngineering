# hello_world_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# This is a simple function to print "Hello, World!"
def print_hello():
    return 'Hello, World!'

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 30),  # Ensure this is a past date when you run it
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'hello_world', 
    default_args=default_args, 
    description='Simple tutorial DAG',
    schedule_interval=timedelta(days=1),  # Or use '@daily' to run it daily
    catchup=False
)

# Define the task using PythonOperator
hello_task = PythonOperator(
    task_id='print_hello', 
    python_callable=print_hello, 
    dag=dag
)

# If you want to add more tasks, you can define them here and set their order using the bitshift operators
# For example, another_task >> hello_task

# In this simple example, there is only one task, so no need to define upstream or downstream dependencies.
