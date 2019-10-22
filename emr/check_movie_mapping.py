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


def main(bucket, output_dir, ml_dir):
    """Check movie dataset mapping is correct
    TMDB and MovieLens movie tiles are confirmed
    to be correctly mapped.
    """
    spark = create_spark_session()

    df_ml_movie = spark.read.csv(
        "s3a://{}/{}/{}".format(bucket, ml_dir, "movies.csv")).persist()

    df_res_movie = spark.read.csv(
        "s3a://{}/{}/{}".format(bucket, output_dir, "movies.csv")).persist()

    target_id = 8012
    ml_row = df_ml_movie.filter(df_ml_movie["movieId"] == target_id).collect()[0]
    result_row = df_res_movie.filter(df_res_movie["ml_movie_id"] == target_id).collect()[0]

    error_message = "Title of MovieLens row may not be correct at movieId=" + str(target_id)
    assert ml_row["title"]  == "Kikujiro (Kikujirô no natsu) (1999)" , error_message
    assert result_row["title"]  == "Kikujiro" , error_message


    target_id = 169518
    ml_row = df_ml_movie.filter(df_ml_movie["movieId"] == target_id).collect()[0]
    result_row = df_res_movie.filter(df_res_movie["ml_movie_id"] == target_id).collect()[0]

    error_message = "Title of MovieLens row may not be correct at movieId=" + str(target_id)
    assert ml_row["title"]  == "Enemy of the Reich: The Noor Inayat Khan Story (2014)" , error_message
    assert result_row["title"]  == "Enemy of the Reich: The Noor Inayat Khan Story" , error_message


if __name__ == '__main__':
    spark = create_spark_session()
    spark.setLogLevel("WARN")

    parser = argparse.ArgumentParser(description='check data count')

    parser.add_argument('--bucket', help='s3 bucket name', required=True)
    parser.add_argument('--output_dir', help='s3 path where result files will be stored', required=True)
    parser.add_argument('--ml_dir', help='s3 path where movielens data is stored', required=True)

    args = parser.parse_args()

    bucket = args.bucket
    output_dir = args.output_dir
    target_name = args.target_name
    main(bucket, output_dir, target_name)
