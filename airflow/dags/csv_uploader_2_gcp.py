import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.gcp import upload_file_2_bucket
from utils.helper import file_checker

sys.path.append('../yellow_taxi_gcp')

with DAG(
        dag_id='GCP_Uploader',
        schedule_interval='0 11 * * *',
        start_date=datetime(2021, 3, 25),
        description='Upload files in GCP Bucket',
        catchup=False,
) as dag:
    file_checker = PythonOperator(
        task_id='file_checker',
        python_callable=file_checker,
        provide_context=True
    )
    csv_uploader = PythonOperator(
        task_id='csv_uploader',
        python_callable=upload_file_2_bucket
    )
