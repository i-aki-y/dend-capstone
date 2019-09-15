import sys
import os
from datetime import datetime
import argparse

import boto3
import botocore

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, flatten, explode, collect_list, concat_ws
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear, date_format,
                                   to_timestamp, from_unixtime)

def create_spark_session():
    """Create spark session"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = create_spark_session()
    spark.setLogLevel("WARN")

    parser = argparse.ArgumentParser(description='run etl process')

    parser.add_argument('--bucket', help='s3 bucket name', required=True)
    parser.add_argument('--tmdb_dir', help='s3 path where tmdb data is stored', required=True)
    parser.add_argument('--ml_dir', help='s3 path where movielens data is stored', required=True)
    parser.add_argument('--output_dir', help='s3 path where result files will be stored', required=True)

    args = parser.parse_args()

    bucket = args.bucket
    tmdb_dir = args.tmdb_dir,
    ml_dir = args.ml_dir
    output_dir = args.output_dir
