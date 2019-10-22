import logging
import time
import json
from datetime import datetime, timedelta
import tempfile

import psycopg2
from botocore.exceptions import ClientError

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class UploadTmdbOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_region="",
                 tmdb_data_path="",
                 conn_config=None,
                 only_current_date=False,
                 *args, **kwargs):

        super(UploadTmdbOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.bucket = s3_bucket
        self.region = s3_region
        self.tmdb_data_path = tmdb_data_path
        self.conn_config = conn_config
        self.only_current_date = only_current_date

    def min_max_timestamp(self):
        """ Get minimum and maximum timestamp of downloaded tmdb data table
        """
        try:
            # PostgreSQLに接続する
            conn = psycopg2.connect(**self.conn_config)
            table_name = 'themoviedb'
            logging.debug("get tmdb_data")

            smt = "SELECT max(ts), min(ts) from {} where http_status = 200".format(
                table_name
            )
            logging.info("query %s", smt)
            cur = conn.cursor()
            cur.execute(smt)
            res = cur.fetchall()
        except psycopg2.Error as e:
            logging.exception("%s", e)
            raise ValueError("", e)
        finally:
            conn.close()

        max_date, min_date = res[0]
        return (min_date, max_date)

    def extract_json_data(self, table_name, dt1, dt2):
        """Select json data from tmdb table in the given timestamp range
        """
        try:
            # connect database
            conn = psycopg2.connect(**self.conn_config)
            conn.set_session(autocommit=True)
            table_name = 'themoviedb'
            logging.debug("get tmdb_data")

            date_fmt = "%Y-%m-%d"
            smt = "SELECT content from {} where ts >= '{}' and ts < '{}' and http_status = 200".format(
                table_name,
                dt1.strftime(date_fmt),
                dt2.strftime(date_fmt)
            )
            logging.info("query %s", smt)
            cur = conn.cursor()
            cur.execute(smt)
            res = cur.fetchall()
        except psycopg2.Error as e:
            logging.exception("%s", e)
            raise ValueError("", e)
        except Exception as e:
            logging.exception("%s", e)
            raise ValueError("", e)

        finally:
            conn.close()

        return res

    def execute(self, context):
        """Extract id list from local DB and upload id list to S3
        """
        if self.only_current_date:
            start_date = context["execution_date"] + datetime.timedelta(days=-1)
            end_date = start_date + datetime.timedelta(days=1)
        else:
            start_date, end_date = self.min_max_timestamp()

        logging.info("%s, %s", start_date, end_date)
        n_date = (end_date - start_date).days + 1
        date_fmt = "%Y-%m-%d"
        hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        s3 = hook.get_session(region_name=self.region).resource('s3')
        my_bucket = s3.Bucket(self.bucket)
        table_name = 'themoviedb'
        logging.info("%s date", n_date)
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(n_date):
                dt1 = start_date + timedelta(days=i)
                dt2 = dt1 + timedelta(days=1)
                res = self.extract_json_data(table_name, dt1, dt2)
                logging.info("result length %s", len(res))
                output_file = "{}.json".format(dt1.strftime(date_fmt))
                local_path = tmp + "/" + output_file
                remote_path = self.tmdb_data_path + "/" + output_file
                logging.info("pathes %s to %s", local_path, remote_path)
                try:
                    with open(local_path, "w") as f:
                        logging.info("write %s", local_path)
                        f.writelines([json.dumps(d[0]) + "\n" for d in res])

                        logging.info("upload from %s to %s", local_path, remote_path)
                        res = my_bucket.upload_file(local_path, remote_path)
                except ClientError as e:
                    logging.error(e)
                    raise ValueError("", e)
                except OSError as e:
                    logging.error(e)
                    raise ValueError("", e)
                except Exception as e:
                    logging.error(e)
                    raise ValueError("", e)

        logging.info("uploaded finished")
