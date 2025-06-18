from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import pandas as pd
import tempfile

def extract_from_snowflake(**context):
    query = "SELECT * FROM DB_2.RAW.EMP"
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    df = snowflake_hook.get_pandas_df(sql=query)

    # Save to temporary CSV file
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(tmp_file.name, index=False)

    # Upload to GCS
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
        bucket_name='data_eng_demos_0',
        object_name='staging/snowflake_data.csv',
        filename=tmp_file.name
    )

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='snowflake_to_bigquery',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['snowflake', 'bigquery'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')


    extract_task = PythonOperator(
        task_id='extract_from_snowflake',
        python_callable=extract_from_snowflake
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='data_eng_demos_0',
        source_objects=['staging/snowflake_data.csv'],
        destination_project_dataset_table='deep-chimera-459105-q6.my_dataset.emp',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # or 'WRITE_APPEND'
        autodetect=True,
    )

    start >> extract_task >> load_to_bq >> end
