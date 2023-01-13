import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models

from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

file_name="flipkart_mobiles.csv"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'project': 'data-pipeline-dev-372513',
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with models.DAG(
        "flipkart-mobile-prices-etl-dag",
        default_args=default_args,
        start_date=datetime.datetime(2021, 1, 1),
        # Not scheduled, trigger only
        schedule_interval=None,
) as dag:
    dataflow_job_file_to_bq = DataflowCreatePythonJobOperator(
        task_id="dataflow_job_file_to_bq",
        py_file="gs://data-pipeline-dev-372513/code/Main.py",
        job_name="mobile-price-file-to-bq-raw",
        options = {
                'project': 'data-pipeline-dev-372513'
                },
        dataflow_default_options = {
            "inputfile": "gs://data-in-trigger-372513/flipkart_mobiles.csv",
            "temp_location": "gs://data-pipeline-dev-372513/temp/",
            "staging_location": "gs://data-pipeline-dev-372513/stage/",
            "region": "us-west1"
            }

    )

    insert_mobile_price_into_trst_table = BigQueryOperator(
        task_id='insert_mobile_price_into_trst_table',
        sql='/SQL/insert_mobile_price_trst.sql',
        use_legacy_sql=False,
    )

    archive_file = GCSToGCSOperator(
        task_id="archive_file",
        source_bucket="data-in-trigger-372513",
        source_object="flipkart_mobiles.csv",
        destination_bucket="data-pipeline-dev-372513",
        destination_object="archive/"+str(datetime.date.today())+"-"+file_name,
        move_object=True,
    )

dataflow_job_file_to_bq >> insert_mobile_price_into_trst_table >> archive_file
