import sys
import os
from datetime import datetime
import argparse

import boto3
import botocore

from pyspark.context import SparkContext
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

def check_s3(client, bucket, prefix):
    """Check whether the given s3 path exist or not
    client: s3 client
    bucket: s3 bucket to check
    prefix: Returns `True` when found item which matches to the `prefix`
    """
    res = client.list_objects(
        Bucket=bucket,
        Prefix=prefix
    )
    return "Contents" in res


def output_df(df, bucket, output_dir, filename):
    df.write.mode('overwrite').csv(
        "s3a://{}/{}/{}.csv".format(
            bucket,
            output_dir,
            filename)
    )


def main(bucket, tmdb_dir, ml_dir, output_dir):
    spark = create_spark_session()
    latest_name = "latest.parquet"

    df_tmdb = spark.read.parquet(f"s3a://{bucket}/{tmdb_dir}/{latest_name}").persist()

    print("tmdb data count: ", df_tmdb.count())
    df_tmdb.printSchema()

    df_tmdb.withColumn(
        "release_date",
        to_timestamp("release_date", "yyyy-MM-dd")
    ).createOrReplaceTempView("tmdb_data")

    print("read links.csv")
    df_link = spark.read.csv(f"s3a://{bucket}/{ml_dir}/links.csv", header=True)
    df_link.printSchema()

    print("read movies.csv")
    df_ml_movie = spark.read.csv(f"s3a://{bucket}/{ml_dir}/movies.csv", header=True)
    df_ml_movie.printSchema()

    print("read ratings.csv")
    df_ml_rate = spark.read.csv(f"s3a://{bucket}/{ml_dir}/ratings.csv", header=True)
    df_ml_rate.printSchema()

    print("create temp views")

    df_link.createOrReplaceTempView("link_data")
    df_ml_movie.createOrReplaceTempView("ml_movie_data")

    df_movie = spark.sql("""
select
    ln.movieId as ml_movie_id
    , tmdb.id as tmdb_movie_id
    , ml.genres as ml_genre
    , year(release_date) as release_year
    , month(release_date) as release_month
    , dayofmonth(release_date) as release_day
    , tmdb.budget as budget
    , tmdb.revenue as revenue
    , tmdb.title as title
    , vote_average as vote_average
    , vote_count as vote_count
    from link_data as ln
    inner join tmdb_data as tmdb on ln.tmdbId = tmdb.id
    inner join ml_movie_data as ml on ml.movieId = ln.movieId
""")

    df_movie.printSchema()

    output_df(df_movie, bucket, output_dir, "movies")

    df_tmdb.select("id", explode("keywords.keywords").alias("kw") ).printSchema()
    df_keywords = spark.sql("""
select
    id
    , concat_ws(' ', collect_list(keyword)) as keywords
from (
    select
        id
        , keyword.name as keyword
    from (
    select
        id
        , explode(keywords.keywords) as keyword
        from tmdb_data
        ) as tmp
    ) as tmp2
group by id
""")

    df_keywords.printSchema()
    df_keywords.limit(5).show()

    df_keywords.createOrReplaceTempView("keyword_data")

    df_search = spark.sql("""
select
    tmdb_data.id as tmdb_id
    , title
    , keyword_data.keywords
    , tagline
    , overview
from tmdb_data
inner join keyword_data on keyword_data.id = tmdb_data.id
""")

    df_search.printSchema()
    df_search.limit(5).show()

    output_df(df_search, bucket, output_dir, "movie_for_search")

    # recreate log_data view
    df_ml_rate.withColumn("ts", from_unixtime("timestamp")).createOrReplaceTempView("ml_rate_data")

    # extract columns to create time table
    df_rating_time = spark.sql("""
SELECT DISTINCT
    timestamp
    , year(ts) as year
    , month(ts) as month
    , cast(date_format(ts, "dd") as INTEGER) as day
    , cast(date_format(ts, "HH") as INTEGER) as hour
    , weekofyear(ts) as week
    , dayofweek(ts) as weekday
FROM ml_rate_data
""")

    df_rating_time.printSchema()
    df_rating_time.limit(5).show()

    output_df(df_rating_time, bucket, output_dir, "rating_time")

    # extract columns to create time table
    df_rating = spark.sql("""
SELECT DISTINCT
    timestamp
    , year(ts) as year
    , month(ts) as month
    , userId as user_id
    , movieId as ml_movie_id
    , rating
FROM ml_rate_data
""")

    df_rating.printSchema()
    df_rating.limit(5).show()
    output_df(df_rating, bucket, output_dir, "rating")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='run etl process')
    parser.add_argument('--loglevel', help='set spark log level')
    parser.add_argument('--bucket', help='s3 bucket name', required=True)
    parser.add_argument('--tmdb_dir', help='s3 path where tmdb data is stored', required=True)
    parser.add_argument('--ml_dir', help='s3 path where movielens data is stored', required=True)
    parser.add_argument('--output_dir', help='s3 path where result files will be stored', required=True)
    args = parser.parse_args()

    sc = SparkContext.getOrCreate()
    loglevel = args.loglevel
    if loglevel is None:
        loglevel = "INFO"
    sc.setLogLevel(loglevel)

    main(args.bucket,
         args.tmdb_dir,
         args.ml_dir,
         args.output_dir)
