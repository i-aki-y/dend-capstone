from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import EmrStepWaitOperator
from helpers import Scripts

default_args = {
    'owner': 'akiy',
    'start_date': datetime(2019, 9, 10),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('process_all_data',
          default_args=default_args,
          description='launches spark cluster and combines movielens and tmdb dataset',
          schedule_interval=None
)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


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
        'pollsecond': 15
    },
)

run_merge_spark_job = BashOperator(
    task_id='run_merge_spark_job',
    dag=dag,
    bash_command=Scripts.spark_merge_all_step_command,
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


start_operator >> launch_cluster
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
