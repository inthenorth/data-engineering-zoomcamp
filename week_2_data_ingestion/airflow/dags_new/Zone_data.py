from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
from ingest_script import ingest_callable
import os
import logging
from airflow.utils.dates import days_ago
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)



 
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



local_workflow = DAG(
    "Zone_data",
    #default_args=default_args,
    start_date = datetime(2018, 7, 24),
    description='test description',
    schedule_interval=None,
    )

url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/misc/'
URL_TEMPLATE =  'taxi+_zone_lookup.csv'
URL_COMPLETE= URL_PREFIX + URL_TEMPLATE
OUTPUT_FILE_TEMPLATE=  '/taxi+_zone_lookup.csv'
OUTPUT_FILE_C= AIRFLOW_HOME + OUTPUT_FILE_TEMPLATE
# TABLE_NAME_TEMPLATE='zone_data_{{ execution_date.strftime(\'%Y_%m\')}}'
parquet_file = URL_TEMPLATE.replace('.csv', '.parquet')

with local_workflow:
     
     wget_task = BashOperator(
         task_id = 'wget',
         bash_command=f'curl -sSLf {URL_COMPLETE} > {OUTPUT_FILE_C} '
         
     )
    
     format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}",
        },
     )
   
     local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/FHV/{parquet_file}",
            "local_file": f"{AIRFLOW_HOME}/{parquet_file}",
        },
    )





    #  ingest_task = PythonOperator(
    #     task_id="ingest",
    #     python_callable=ingest_callable,
    #     op_kwargs= dict(
    #         user=PG_USER, 
    #         password=PG_PASSWORD, 
    #         host=PG_HOST, 
    #         port=PG_PORT, 
    #         db=PG_DATABASE, 
    #         table_name=TABLE_NAME_TEMPLATE, 
    #         csv_file=OUTPUT_FILE_TEMPLATE
    #     ),
           
    # )
          
     wget_task >> format_to_parquet_task >> local_to_gcs_task