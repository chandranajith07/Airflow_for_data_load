from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import pandas as pd
import tempfile



default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='gcs_to_bigquery',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['Gcs', 'bigquery'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='data_eng_demos_0',
        source_objects=['staging/snowflake_data.csv'],
        destination_project_dataset_table='deep-chimera-459105-q6.my_dataset.emp_2',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # or 'WRITE_APPEND'
        autodetect=True,
    )

    start >>  load_to_bq >> end