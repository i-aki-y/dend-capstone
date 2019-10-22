from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (S3UploadOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'akiy',
    'start_date': datetime(2019, 9, 10),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('prepare_movielens_dag',
          default_args=default_args,
          description='upload movielens dataset to project s3 bucket',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

LOCAL_TMP_DIR = Variable.get('local_tmp_dir')
ZIP_FILE_NAME = "ml-latest.zip"
LOCAL_DST = "{}/{}".format(LOCAL_TMP_DIR, ZIP_FILE_NAME)


download_ml_data = BashOperator(
    task_id='download_ml_data',
    dag=dag,
    bash_command='curl {{ params.zip_url }} -o {{ params.local_dst }}',
    params={'zip_url': Variable.get('movielens_zip_url'),
            'local_dst': LOCAL_DST
    }
)

unzip_ml_data = BashOperator(
    task_id='unzip_ml_data',
    dag=dag,
    bash_command='unzip -o {{ params.local_dst }} -d {{ params.local_dir }}',
    params={'local_dst': LOCAL_DST,
            'local_dir': LOCAL_TMP_DIR
    }
)

ML_DATA_PATH = Variable.get('ml_data_path')

upload_rates = S3UploadOperator(
    task_id="upload_rates",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src="{}/ml-latest/ratings.csv".format(LOCAL_TMP_DIR),
    dst="{}/ratings.csv".format(ML_DATA_PATH)
)

upload_movies = S3UploadOperator(
    task_id="upload_movies",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src="{}/ml-latest/movies.csv".format(LOCAL_TMP_DIR),
    dst="{}/movies.csv".format(ML_DATA_PATH)
)

upload_links = S3UploadOperator(
    task_id="upload_links",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src="{}/ml-latest/links.csv".format(LOCAL_TMP_DIR),
    dst="{}/links.csv".format(ML_DATA_PATH)
)

upload_tags = S3UploadOperator(
    task_id="upload_tags",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src="{}/ml-latest/tags.csv".format(LOCAL_TMP_DIR),
    dst="{}/tags.csv".format(ML_DATA_PATH)
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> download_ml_data
download_ml_data >> unzip_ml_data
unzip_ml_data >> upload_rates
unzip_ml_data >> upload_links
unzip_ml_data >> upload_movies
unzip_ml_data >> upload_tags

upload_rates >> end_operator
upload_links >> end_operator
upload_movies >> end_operator
upload_tags >> end_operator
