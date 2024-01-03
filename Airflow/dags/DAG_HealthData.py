from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys 
from pathlib import Path

# Import your workflow functions from RunPubmedWeb.py
from GetPubMedData import MedicalDataWorkflow as mdw 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    # Other default args
}

dag2 = DAG('medical_workflow_three_hourly', default_args=default_args, schedule_interval=timedelta(minutes=5))

task4 = PythonOperator(
    task_id='download_article_text',
    python_callable=mdw.run_download_article_text,
    dag=dag2,
)

task4