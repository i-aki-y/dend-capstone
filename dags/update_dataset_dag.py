from datetime import datetime, timedelta
import os
import logging

import pandas as pd
import requests
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (S3UploadOperator,
                               CollectTmdbOperator,
                               UploadTmdbOperator,
                               EmrStepWaitOperator)

from helpers import Scripts

LOCAL_MOVIE_ID_PATH = "{}/{}".format(
    Variable.get('local_tmp_dir'), "update_movie_ids.csv")

TMDB_DATA_PATH = Variable.get("tmdb_data_path")

default_args = {
    'owner': 'akiy',
    'start_date': datetime(2019, 9, 10),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('update_movie_dataset',
          default_args=default_args,
          description='get tmdb changes and run spark jobs',
          #schedule_interval="0 6 * * *"
          schedule_interval=None
        )


def get_movie_changes(start_date, page, api_key):
    """Get movie changes by using TMDB API

    See, https://developers.themoviedb.org/3/changes/get-movie-change-list

    start_date: start_date parameter of movie/changes api
    page: page parameter of movie/changes api
    api_key: TMDB's api key

    Return: list of movie ids
    """
    start_date_str = start_date.strftime('%Y-%m-%d')
    my_url = f"https://api.themoviedb.org/3/movie/changes?api_key={api_key}&start_date={start_date_str}&page={page}"
    res = requests.get(my_url)
    logging.info("status code %s", res.status_code)
    content = res.json() if res.status_code == 200 else None

    if content is not None:
        ids = [int(item["id"]) for item in content["results"]]
        total_pages = int(content["total_pages"])
        if  total_pages > page:
            logging.info("total_pages %s, cur_page %s", total_pages, page)
            ids += get_movie_changes(start_date_str, page+1, api_key)
        return ids
    return []


def prepare_movie_changes(id_file, api_key, **context):
    """Get changed movie IDs from tmdb api
    id_file: file path to store obtained id list
    api_key: tmdb api key
    """
    start_date = context["execution_date"] + timedelta(days=-1)
    logging.info("request chage at %s", start_date)
    ids = get_movie_changes(start_date, 1, api_key)
    logging.info("obtained id counts %s", len(ids))
    pd.DataFrame(ids).to_csv(id_file, index=False, header=False)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


prepare_tmdb_movie_change = PythonOperator(
    task_id='get_tmdb_movie_change',
    dag=dag,
    python_callable=prepare_movie_changes,
    provide_context=True,
    op_kwargs={
        "id_file": LOCAL_MOVIE_ID_PATH,
        "api_key": Variable.get("tmdb_api_key")
    }
)

upload_movie_ids = S3UploadOperator(
    task_id="upload_update_movie_ids",
    dag=dag,
    aws_credentials_id='aws_credentials',
    region=Variable.get('aws_region'),
    bucket=Variable.get('s3_bucket'),
    src=LOCAL_MOVIE_ID_PATH,
    dst="{}/update_movie_ids.csv".format(TMDB_DATA_PATH)
)

collect_updated_tmdb_movie = CollectTmdbOperator(
    task_id="collect_updated_tmdb_movie",
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
    only_current_date=True
)


launch_cluster = BashOperator(
    task_id='launch_cluster',
    dag=dag,
    bash_command=Scripts.launch_cluster_command,
    params={
        'key_name': Variable.get('key_pair'),
        'subnet_id': Variable.get('subnet_id'),
        'release_label': Variable.get('emr_release_label'),
        'log_url': Variable.get('emr_log_url'),
        'bootstrap_action': Variable.get('emr_bootstrap_action'),
        'region': Variable.get('aws_region')
    },
    xcom_push=True
)

wait_cluster_setup = BashOperator(
    task_id='wait_cluster_setup',
    dag=dag,
    bash_command=Scripts.wait_cluster_setup_command2,
    params={
        'region': Variable.get('aws_region'),
    },
)

run_merge_spark_job = BashOperator(
    task_id='run_merge_spark_job',
    dag=dag,
    bash_command=Scripts.spark_merge_one_day_step_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/merge_dataset.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'tmdb_dir': Variable.get("tmdb_data_path"),
        'update_prefix': "update/"
    },
    xcom_push=True
)

run_etl_spark_job = BashOperator(
    task_id='run_etl_spark_job',
    dag=dag,
    bash_command=Scripts.spark_etl_step_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/etl.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'tmdb_dir': Variable.get("tmdb_data_path"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results"
    },
    xcom_push=True
)


wait_step = EmrStepWaitOperator(
    task_id='wait_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="run_etl_spark_job",
    timeout_min=240
)

check_movie_data_count = BashOperator(
    task_id='check_movie_data_count',
    dag=dag,
    bash_command=Scripts.spark_check_data_count_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/check_count.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results",
        'target_name': "movie.csv"
    },
    xcom_push=True
)

wait_movie_check_step = EmrStepWaitOperator(
    task_id='wait_movie_check_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="check_movie_data_count",
    timeout_min=60
)

check_movie_mapping = BashOperator(
    task_id='check_movie_mapping',
    dag=dag,
    bash_command=Scripts.spark_check_movie_mapping_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/check_movie_mapping.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results",
    },
    xcom_push=True
)

wait_movie_mapping_check_step = EmrStepWaitOperator(
    task_id='wait_movie_mapping_check_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="check_movie_mapping",
    timeout_min=60
)


check_rating_data_count = BashOperator(
    task_id='check_rating_data_count',
    dag=dag,
    bash_command=Scripts.spark_check_data_count_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/check_count.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results",
        'target_name': "rating.csv"
    },
    xcom_push=True
)

wait_rating_check_step = EmrStepWaitOperator(
    task_id='wait_rating_check_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="check_rating_data_count",
    timeout_min=60
)


check_rating_time_data_count = BashOperator(
    task_id='check_rating_time_data_count',
    dag=dag,
    bash_command=Scripts.spark_check_data_count_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/check_count.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results",
        'target_name': "rating_time.csv"
    },
    xcom_push=True
)

wait_rating_time_check_step = EmrStepWaitOperator(
    task_id='wait_rating_time_check_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="check_rating_time_data_count",
    timeout_min=60
)

check_movie_for_search_data_count = BashOperator(
    task_id='check_movie_for_search_data_count',
    dag=dag,
    bash_command=Scripts.spark_check_data_count_command,
    params={
        'region': Variable.get('aws_region'),
        'script_path': "s3a://{}/check_count.py".format(Variable.get("s3_bucket")),
        'bucket': Variable.get("s3_bucket"),
        'ml_dir': Variable.get("ml_data_path"),
        'output_dir': "results",
        'target_name': "movie_for_search.csv"
    },
    xcom_push=True
)

wait_movie_for_search_check_step = EmrStepWaitOperator(
    task_id='wait_movie_for_search_check_step',
    dag=dag,
    aws_credentials_id="aws_credential",
    region=Variable.get('aws_region'),
    cluster_task_id="launch_cluster",
    step_task_id="check_movie_for_search_data_count",
    timeout_min=60
)

terminate_cluster = BashOperator(
    task_id='terminate_cluster',
    dag=dag,
    bash_command=Scripts.terminate_cluster_command,
    params={
        'region': Variable.get('aws_region'),
    },
    trigger_rule="all_done"
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> prepare_tmdb_movie_change
prepare_tmdb_movie_change >> upload_movie_ids
upload_movie_ids >> collect_updated_tmdb_movie
collect_updated_tmdb_movie >> upload_result
upload_result >> launch_cluster
launch_cluster >> wait_cluster_setup
wait_cluster_setup >> run_merge_spark_job
run_merge_spark_job >> run_etl_spark_job
run_etl_spark_job >> wait_step

wait_step >> check_movie_mapping
check_movie_mapping >> wait_movie_mapping_check_step
wait_movie_mapping_check_step >> terminate_cluster

wait_step >> check_movie_data_count
check_movie_data_count >> wait_movie_check_step
wait_movie_check_step >> terminate_cluster

wait_step >> check_rating_data_count
check_rating_data_count >> wait_rating_check_step
wait_rating_check_step >> terminate_cluster

wait_step >> check_rating_time_data_count
check_rating_time_data_count >> wait_rating_time_check_step
wait_rating_time_check_step >> terminate_cluster

wait_step >> check_movie_for_search_data_count
check_movie_for_search_data_count >> wait_movie_for_search_check_step
wait_movie_for_search_check_step >> terminate_cluster

terminate_cluster >> end_operator
