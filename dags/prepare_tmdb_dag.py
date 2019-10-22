from datetime import datetime, timedelta
import os
import sys

from datetime import datetime

import logging

import psycopg2


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (S3UploadOperator,
                               CollectTmdbOperator,
                               UploadTmdbOperator)

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

dag = DAG('prepare_tmdb_dag',
          default_args=default_args,
          description='collect tmdb dataset and upload to project s3 bucket',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


MOVIE_LIST_FILE_NAME = "movie_ids_08_20_2019.json.gz"
REMOTE_SRC = "http://files.tmdb.org/p/exports/{}".format(MOVIE_LIST_FILE_NAME)
LOCAL_TMP_DIR = Variable.get('local_tmp_dir')
LOCAL_DST = "{}/{}".format(LOCAL_TMP_DIR, MOVIE_LIST_FILE_NAME)
LOCAL_MOVIE_ID_PATH = "{}/movie_ids.csv".format(LOCAL_TMP_DIR)
TMDB_DATA_PATH = Variable.get("tmdb_data_path")


download_tmdb_movie_list = BashOperator(
    task_id='download_tmdb_movie_list',
    dag=dag,
    bash_command='curl {{ params.movie_ids_url }} -o {{ params.local_dst }}',
    params={'movie_ids_url': REMOTE_SRC,
            'local_dst': LOCAL_DST
    }
)

make_tmdb_movie_list = BashOperator(
    task_id='make_tmdb_movie_list',
    dag=dag,
    bash_command='gunzip -c {{ params.local_dst }} | jq .id | {{ params.movie_ids }}',
    params={'local_dst': LOCAL_DST,
            'movie_ids': LOCAL_MOVIE_ID_PATH
    }
)


upload_movie_ids = S3UploadOperator(
    task_id="upload_movie_ids",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src=LOCAL_MOVIE_ID_PATH,
    dst="{}/movie_ids.csv".format(TMDB_DATA_PATH)
)


def init_db(conn_config):
    """Initialize database
    This create `themoviedb` table in the database
    """
    try:
        # PostgreSQLに接続する
        conn = psycopg2.connect(**conn_config)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        table_name = 'themoviedb'
        logging.info("Init table %s", table_name)
        # create sparkify database with UTF8 encoding
        cur.execute("DROP TABLE IF EXISTS {}".format(table_name))
        cur.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        tmdb_id integer,
        http_status integer,
        ts timestamp,
        content json
        )
        '''.format(table_name))
    except Exception as e:
        logging.exception("%s", e)
    finally:
        conn.close()


init_db = PythonOperator(
    task_id="initialize_tmdb_db",
    dag=dag,
    python_callable=init_db,
    op_kwargs={"conn_config": Variable.get("pg_config", deserialize_json=True)}
)

#
collect_tmdb_movie = CollectTmdbOperator(
    task_id="collect_full_tmdb_movie",
    dag=dag,
    id_file=LOCAL_MOVIE_ID_PATH,
    conn_config=Variable.get("pg_config", deserialize_json=True),
    tmdb_api_key=Variable.get("tmdb_api_key"),
)


upload_result = UploadTmdbOperator(
    task_id="upload_collected_tmdb_data",
    dag=dag,
    aws_credentials_id="aws_credential",
    s3_bucket=Variable.get("s3_bucket"),
    s3_region=Variable.get("aws_region"),
    tmdb_data_path="{}/{}".format(Variable.get("tmdb_data_path"), "update"),
    conn_config=Variable.get("pg_config", deserialize_json=True),
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> download_tmdb_movie_list
start_operator >> init_db
download_tmdb_movie_list >> make_tmdb_movie_list
make_tmdb_movie_list >> upload_movie_ids
upload_movie_ids >> collect_tmdb_movie
init_db >> collect_tmdb_movie
collect_tmdb_movie >> upload_result
upload_result >> end_operator
