from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

dags_dir = Path(__file__).parent
parent_dir = dags_dir.parent
sys.path.append(str(parent_dir))

# Import your workflow functions from RunPubmedWeb.py
from RunPubmedWeb import run_health_terms, run_medical_conditions, run_get_pmids, run_download_article_text

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    # Other default args
}

dag = DAG('medical_workflow', default_args=default_args, schedule_interval='@daily')

# Define the tasks
task1 = PythonOperator(
    task_id='get_health_terms',
    python_callable=run_health_terms,
    dag=dag,
)

task2 = PythonOperator(
    task_id='get_medical_conditions',
    python_callable=run_medical_conditions,
    dag=dag,
)

task3 = PythonOperator(
    task_id='get_pmids',
    python_callable=run_get_pmids,
    dag=dag,
)

task4 = PythonOperator(
    task_id='download_article_text',
    python_callable=run_download_article_text,
    dag=dag,
)

# Set dependencies
task1 >> task2 >> task3 >> task4