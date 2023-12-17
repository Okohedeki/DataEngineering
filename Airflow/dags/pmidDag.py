from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
from pathlib import Path


dags_dir = Path(__file__).parent
parent_dir = dags_dir.parent
sys.path.append(str(parent_dir))
# Import your workflow functions from RunPubmedWeb.py
from RunPubmedWeb import run_health_terms, run_medical_conditions, run_get_pmids


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    # Other default args
}

dag1 = DAG('medical_workflow_hourly', default_args=default_args, schedule_interval='@hourly')

task1 = PythonOperator(
    task_id='get_health_terms',
    python_callable=run_health_terms,
    dag=dag1,
)

task2 = PythonOperator(
    task_id='get_medical_conditions',
    python_callable=run_medical_conditions,
    dag=dag1,
)

task3 = PythonOperator(
    task_id='get_pmids',
    python_callable=run_get_pmids,
    dag=dag1,
)

task1 >> task2 >> task3