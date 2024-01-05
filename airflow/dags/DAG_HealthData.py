from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys 
from pathlib import Path
import pendulum

# Import your workflow functions from RunPubmedWeb.py
from GetPubMedData import MedicalDataWorkflow 
mdw = MedicalDataWorkflow() 
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2022, 12, 31, tz="US/Arizona"),
    # Other default args
}

dag2 = DAG('Test2', default_args=default_args, schedule_interval=timedelta(minutes=1),  catchup=False)

task4 = PythonOperator(
    task_id='download_article_text',
    python_callable=mdw.run_download_article_text,
    dag=dag2,
)

task4