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

def merge_dataset(spark, df_latest, df_update, client, bucket, tmp_path, latest_path):
    """Merge latest data and additional data
    df_latest: Dataframe of latest dataset
    df_update: Dataframe of addition dataset
    client: s3 client
    bucket: Project s3 bucket
    tmp_path: Temporary path name used to save merged data
    latest_path: Path of latest data is saved
    """

    df_latest.createOrReplaceTempView("data_latest")
    df_update.createOrReplaceTempView("data_update")

    # Data merge is done by a following steps
    # 1. Filter latest data with id which included in update data
    # 2. Concate update data
    df_merged = spark.sql("""
select
    *
from
    data_latest
where id not in (select id from data_update)

union

select
    *
from
    data_update

""")

    df_merged.persist()
    print("merged dataset")
    print("size: ",df_merged.count())
    print("Write merged data to: ", latest_path)
    df_merged.write.mode('overwrite').parquet(
        f"s3a://{bucket}/{latest_path}")
    return df_merged


def merge_latest(spark, bucket, tmdb_dir, update_prefix):
    """Merge the latest TMDB data with additional TMDB data
    Additional TMDB data is uploaded in update folder in project s3 bucket.
    The merged data is saved as the latest TMDB date and previous latest data is overwritten.
    """

    s3client = boto3.client('s3')

    latest_name = "latest.parquet"
    tmp_path = f"{tmdb_dir}/tmp.parquet"
    latest_path = f"{tmdb_dir}/{latest_name}"
    has_latest = check_s3(s3client, bucket,  latest_path)
    has_update = False if update_prefix is None else check_s3(s3client, bucket, f"{tmdb_dir}/{update_prefix}")

    if has_latest:
        print("Load latest data")
        df_tmdb_latest = spark.read.parquet(f"s3a://{bucket}/{tmdb_dir}/{latest_name}").persist()
        print("size: ", df_tmdb_latest.count())
    if has_update:
        ## update data are uploaded as json files
        print("Load update files")
        df_tmdb_update = spark.read.json(f"s3a://{bucket}/{tmdb_dir}/{update_prefix}*").persist()
        print("size: ", df_tmdb_update.count())

    sys.stdout.flush()

    if has_latest and has_update:
        print("merge data")
        df_tmdb_latest = merge_dataset(spark, df_tmdb_latest, df_tmdb_update, s3client, bucket, tmp_path, latest_path)

    elif has_latest and not has_update:
        print("no merge")
        pass

    elif (not has_latest) and has_update:
        print("Any latest data is not found. Use update data as latest")
        df_tmdb_latest = df_tmdb_update

        write_path = f"s3a://{bucket}/{tmdb_dir}/{latest_name}"

        print("Write to: ", write_path)
        df_tmdb_latest.write.mode('overwrite').parquet(write_path)

    else:

        raise OSError("no dataset found")

    return df_tmdb_latest


def main(bucket, tmdb_dir, ml_dir, update_prefix, output_dir):
    spark = create_spark_session()

    df_tmdb = merge_latest(spark, bucket, tmdb_dir, update_prefix)

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

    df_movie.write.mode('overwrite').parquet(f"s3a://{bucket}/{output_dir}/movies.parquet")


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

    df_search.write.mode('overwrite').parquet(f"s3a://{bucket}/{output_dir}/movie_for_search.parquet")


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

    df_rating_time.write.mode('overwrite').partitionBy("year").parquet(
        f"s3a://{bucket}/{output_dir}/rating_time.parquet")

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
    df_rating.write.mode('overwrite').partitionBy("year", "month").parquet(
        f"s3a://{bucket}/{output_dir}/rating.parquet")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='run etl process')
    parser.add_argument('--loglevel', help='set spark log level')
    parser.add_argument('--bucket', help='s3 bucket name', required=True)
    parser.add_argument('--tmdb_dir', help='s3 path where tmdb data is stored', required=True)
    parser.add_argument('--ml_dir', help='s3 path where movielens data is stored', required=True)
    parser.add_argument('--output_dir', help='s3 path where result files will be stored', required=True)
    parser.add_argument('--update_prefix', help='prefix which matches tmdb update files')
    args = parser.parse_args()

    sc = SparkContext.getOrCreate()
    loglevel = args.loglevel
    if loglevel is None:
        loglevel = "INFO"
    sc.setLogLevel(loglevel)

    main(args.bucket,
         args.tmdb_dir,
         args.ml_dir,
         args.update_prefix,
         args.output_dir)
