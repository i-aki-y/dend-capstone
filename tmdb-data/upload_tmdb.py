import os
import json
from datetime import datetime, timedelta
import psycopg2
from boto3.session import Session
from botocore.exceptions import ClientError
import tempfile

from logging import basicConfig, getLogger, DEBUG

basicConfig(level=DEBUG)
logger = getLogger(__name__)

BUCKET_NAME = os.environ["PROJ_BUCKET"]
TMDB_DATA_PATH = os.environ["TMDB_DATA_PATH"]

PG_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '',
    'host': 'localhost',
    'port': '5432',
}


def min_max_timestamp():
    """ Get minimum and maximum timestamp of downloaded tmdb data table
    """
    try:
        # PostgreSQLに接続する
        conn = psycopg2.connect(**PG_CONFIG)
        table_name = 'themoviedb'
        logger.debug("get tmdb_data")

        smt = "SELECT max(ts), min(ts) from {} where http_status = 200".format(
            table_name
        )
        logger.info("query %s", smt)
        cur = conn.cursor()
        cur.execute(smt)
        res = cur.fetchall()
    except psycopg2.Error as e:
        logger.exception("%s", e)
    finally:
        conn.close()

    min_date, max_date = res[0]

    return (min_date, max_date)


def extract_json_data(table_name, dt1, dt2):
    """Select json data from tmdb table in the given timestamp range
    """
    try:
        # connect database
        conn = psycopg2.connect(**PG_CONFIG)
        conn.set_session(autocommit=True)
        table_name = 'themoviedb'
        logger.debug("get tmdb_data")

        date_fmt = "%Y-%m-%d"
        smt = "SELECT content from {} where ts >= '{}' and ts < '{}' and http_status = 200".format(
            table_name,
            dt1.strftime(date_fmt),
            dt2.strftime(date_fmt)
        )
        logger.info("query %s", smt)
        cur = conn.cursor()
        cur.execute(smt)
        res = cur.fetchall()
    except psycopg2.Error as e:
        logger.exception("%s", e)
    finally:
        conn.close()

    return res


def main():
    """Upload json file stored databse for each timestamp date
    """
    start_date, end_date = min_max_timestamp()
    n_date = (end_date - start_date).days + 1
    date_fmt = "%Y-%m-%d"
    session = Session(aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                      region_name='us-west-2')
    s3 = session.resource('s3')
    bucket = s3.Bucket(os.environ["S3_BUCKET"])
    table_name = 'themoviedb'

    with tempfile.TemporaryDirectory() as tmp:
        for i in range(n_date):
            dt1 = start_date
            dt2 = dt1 + timedelta(days=i+1)
            res = extract_json_data(table_name, dt1, dt2)
            output_file = "{}.json".format(dt1.strftime(date_fmt))
            local_path = tmp + "/" + output_file
            remote_path = TMDB_DATA_PATH + "/" + output_file
            try:
                with open(local_path, "w") as f:
                    logger.info("write %s", local_path)
                    f.writelines([json.dumps(d[0]) + "\n" for d in res])

                logger.info("upload from %s to %s", local_path, remote_path)
                res = bucket.upload_file(local_path, remote_path)
            except ClientError as e:
                logger.error(e)
            except OSError as e:
                logger.error(e)


if __name__ == "__main__":
    main()
