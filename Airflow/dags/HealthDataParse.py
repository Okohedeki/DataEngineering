from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from RunPubmedWeb import run_download_article_text
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

dag2 = DAG('medical_workflow_three_hourly', default_args=default_args, schedule_interval=timedelta(hours=4))

task4 = PythonOperator(
    task_id='download_article_text',
    python_callable=run_download_article_text,
    dag=dag2,
)

task4