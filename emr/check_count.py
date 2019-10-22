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


def main(bucket, output_dir, target_name):
    """Check result dataset counts
    The dataset count shuold larger than 0
    """
    spark = create_spark_session()

    df = spark.read.csv(
        "s3a://{}/{}/{}".format(bucket, output_dir, target_name), header=True).persist()

    assert df.count() > 0, "result movie data is empty"


if __name__ == '__main__':
    spark = create_spark_session()
    spark.setLogLevel("WARN")

    parser = argparse.ArgumentParser(description='check data count')

    parser.add_argument('--bucket', help='s3 bucket name', required=True)
    parser.add_argument('--output_dir', help='s3 path where result files will be stored', required=True)
    parser.add_argument('--target_name', help='target file name for the check', required=True)

    args = parser.parse_args()

    bucket = args.bucket
    output_dir = args.output_dir
    target_name = args.target_name
    main(bucket, output_dir, target_name)
