import logging
import time
import re
from datetime import datetime

import requests
import psycopg2
import pandas as pd

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StepTimeoutError(Exception):
    pass

class EmrStepWaitOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 region="",
                 cluster_task_id="",
                 step_task_id="",
                 timeout_min=120,
                 *args, **kwargs):

        super(EmrStepWaitOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.cluster_task_id = cluster_task_id
        self.step_task_id = step_task_id
        self.timeout_min = timeout_min
        self.start_time = datetime.now()

    def execute(self, context):
        hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        client = hook.get_client_type("emr", region_name=self.region)
        cluster_id = context['task_instance'].xcom_pull(task_ids=self.cluster_task_id)
        steps_str = context['task_instance'].xcom_pull(task_ids=self.step_task_id)
        step_ids = re.sub("\"|'", "", steps_str).split()
        step_info = client.list_steps(ClusterId=cluster_id, StepIds=step_ids)
        step_status = step_info['Steps'][-1]['Status']['State']

        while step_status in ['PENDING', 'RUNNING']:
            step_info = client.list_steps(ClusterId=cluster_id, StepIds=step_ids)
            step_status = step_info['Steps'][-1]['Status']['State']
            if self.is_timeout():
                raise StepTimeoutError
            time.sleep(60)

    def is_timeout(self):
        dt = (datetime.now() - self.start_time)
        return dt.seconds > self.timeout_min * 60
