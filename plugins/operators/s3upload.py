import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class S3UploadOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 bucket="",
                 region="",
                 src="",
                 dst="",
                 *args, **kwargs):

        super(S3UploadOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.bucket = bucket
        self.region = region
        self.src = src
        self.dst = dst

    def execute(self, context):
        hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        s3 = hook.get_session(region_name=self.region).resource('s3')
        my_bucket = s3.Bucket(self.bucket)
        logging.info("upload files from %s to %s/%s/", self.src, my_bucket, self.dst)
        my_bucket.upload_file(self.src, self.dst)
