## Purpose of the project

In this project, I will provide an data model where users can analyze movie review data.
We can access several open movie data across the world [^tmdb][^imdb][^movielens].
While these data can be used for research of recommendation or text analysis, each dataset have own advantage and disadvantage.
For example Movie Lens is a famous dataset which contains many anonymized user ratings about movies. But the dataset have little data about movie's features such as actors or revenue.
TMDB is another movie dataset which contains details of movie data. Since movie lens's dataset provide `link.csv` which contains references to other public dataset such as TMDB or IMDB, we can combine these dataset into the single data model which enhances the analysis process. So in this project I will make a pipe line which combines different movie dataset to a single data model.

In this project, I think of two usecases for this data model. One is a creating recommendation model. This is a main purpose of Movie Lens dataset. Another usecase is a search index. Fulltext search is ubiquitous around many services.

Note that the scope of this project is to prepare the well arranged dataset. Therefore this project does not have any recommendation and indexing logics.

## Steps of the project

This project contains the following steps:

1. Collect TMDB data from the API
2. Upload collected data to S3
3. Upload MovieLens dataset to S3
4. Combine MovieLens and TMDB dataset using Apache Spark
5. Save processed data to S3
6. Collect TMDB data change set.
7. Update the result dataset.

From 1 to 5 steps should be executed only one time, and the 6 and 7 steps are executed daily.

### 1. Collect TMDB data through the TMBD API

Movie data in the TMDB is provided with API.
Since the API returns a single movie data from a request.
I should collect movie data using successive API calls.

We can get the latest movie list from `http://files.tmdb.org/p/exports/` endpoint[^tmbdex]. Since the TMDB have over the 480K movie data and the API call is limited 40 requests per 10 seconds, it takes 1.5 days ($1.5 \approx 480K/(24*3600*4)$) to obtain all movie data. The returned data stored temporarily local database.

### 2. Upload collected data to S3

The collect TMDB data are uploaded to the S3 storage. I upload the data periodically for each 10K items.

### 3. Upload MovieLens dataset to S3

I use "MovieLens Latest Datasets" which contains _27,000,000 ratings and 1,100,000 tag applications applied to 58,000 movies by 280,000 users. Includes tag genome data with 14 million relevance scores across 1,100 tags. Last updated 9/2018._ [^movielens].

The data is provided as a zip file including some csv files.
We can download the file, unzip and upload the resulted csv file to the S3.

### 4. Combine MovieLens and TMDB dataset using Apache Spark

In order to parse TMDB's json data, I use apache spark [^spark]. I create spark environment in EMR service in the AWS[^emr].

### 5. Save processed data to S3

Processed data are saved in Parquet format in S3[^parquet].

### 6. Collect TMDB data change set

Although MovieLens data does not update frequently, TMDB's dataset is constantly updated.
In this step, the changes of TMDB's dataset are obtained from the API.

### 7. Update the result dataset

We merge the change set to our result dataset.

## Contents

This project contains the following files

- `README.md`: This file. This contains the description of the project and usage.
- `airflow_variables.json `: This defines the airflow variables which are used in this project. This file includes some *place holders* which should appropriately be replaced according to your environment. You can import this file from the airflow web interface by the following steps: Select menus `Admin` > `Variable`, and click `Import Variables`.
- `imgs/*`: Supplementary materials used in `README.md`.
- `dag/*.py` : These are definitions of airflow dags.
- `plugins/helpers/scripts.py`: In this file, there are some definitions of shell scripts in which we setup EMR cluster and control the steps.
- `plugins/operators/*.py`: These files defines airflow's custom operators.
- `emr/emr_bootstrap.sh`: This is a bootstrap script which is used when the EMR cluster is setup. In order to run cluster set script, you should upload this script to the S3 where the EMR process can access.
- `emr/etl.py`: This is a ETL script which is executed on the EMR cluster.

## Airflow pipeline

### run airflow

This project is implemented as a airflow pipeline.
After the setup the airflow[^airflow_setup], you should run the following command.

```
$ airflow webservar
$ airflow scheduler
```

Edit the following variables in the `${AIRFLOW_HOME}/airflow.cfg`.

```
dags_folder = ${PROJECT_ROOT}/dag
plugins_folder = ${PROJECT_ROOT}/plugins
```
The `${PROJECT_ROOT}` is correspond to the directory which contains this `README.md`.

### DAG description

In this project, I defined the following pipelines.

- `prepare_movielens_dag`: This dag download the latest MovieLens data and upload the data to the S3.
- `prepare_tmdb_dag`: This dag collect TMDB data from the TMBD API and upload the data to the S3.
- `process_all_data`: This dag setup aws EMR cluster and runs etl process by using spark.
- `update_movie_dataset`: This dag collect daily changeset from TMDB API and merge it to the latest result dataset by using the EMR cluster.

## Scenarios

### "the data was increased by 100x"

Now, each of the MovieLens dataset is provided as a single file, but they can be
split multiple files and can be uploaded to S3. The S3 is capable for very large data with low cost. In this project, the raw data files are easily loaded by using spark's `read.csv(path/*)`. In the spark, the dataset are processed in the multiple partitions. We can scale up the aws EC2 instance or scale out by increasing partition number of spark process.

### "the pipelines would be run on a daily basis by 7 am every day."

In order to collect the whole TMDB's movie data, it takes 1.5 days. But once we got the data, TMDB provide `/movie/changes` API which returns a list of movie ids that have changed in past 24 hours.
The list looks have $\sim 2k$ items. When we request movie data 3 items per second, we can collect the changed movie list in 2 hours.
In this pipe line, the changed movie list will be uploaded S3 as a update file. And before the etl process, this update file will be merged with the current raw dataset. So we can complete the pipe line in daily bases.

### "The database needed to be accessed by 100+ people."

The resulted data are stored in S3 of AWS. The datamodel is made for data analysis which is read intensive usecase. S3 is capable for 5,500 requests per second[^s3spec].
In this pipeline, I use AWS EMR with Jupyterhub as a Spark environment. The Jupyterhub serves jupyter notebook for multiple users. With this configuration, I think the data model can be available for 100+ people.


## Data quality check

- Check that the same movie are appropriately joined

I checked that sampled MovieLens data movie and TMDB movie have same movie title.
See, `emr/check_movie_mapping.py`

- Check all data not empty

Check the resulted data have non zero data.
See, `emr/check_count.py`

## About Dataset

As source of data, I use the following dataset.

- MovieLens
- The Movie DataBase

### Data format

Two data format are used.
CSV: Dataset of MovieLens are provided as CSV.
API: TMDB data are provided by API.

### Number of data

MovieLens's `rating.csv` have 27,753,445 lines.

## Dataset details

### MovieLens

In addition to ratings, MovieLens provide some dataset.
Here, I use the following data.

- movies.csv
- links.csv
- ratings.csv

### The Movie Database (TMDB)

The Movie Database provide many movie features through their API such as review of users. By using the `append_to_response` url parameter, we can get related information in a single movie detail request.
I collect the following information from the API.

- movies
- keywords
- reviews

### Dataset dictionary

The result dataset are stored in S3 bucket as csv files.
The `movies.csv`, `rating.csv` and `rating_time.csv` are assumed to use develope recommendation model.
The `movie_for_search.csv` is assumed to create search index.

- movies : Movie dimension table 
  - ml_movie_id: movie lens's movie ID
  - tmdb_movie_id: tmdb movie ID
  - ml_genre: movie lens's genre
  - release_year: movie's release year
  - release_month: movie's release month
  - release_day: movie's release day
  - budget: movie's budget
  - revenue: movie's revenue
  - title: movie's title
  - vote_average: tmdb's vote average
  - vote_count: tmdb's vote count

- rating : Fact table
  - user_id: movie lens's user ID
  - ml_movie_id: movie lens's movie ID
  - timestamp: timestamp when the user rating the movie.
  - rating: rating score

- rating_time : Timestamp dimension table
  - timestamp: timestamp when the user rating the movie.
  - year: year of the timestamp 
  - month: month of the timestamp 
  - day: day of the timestamp 
  - hour: hour of the timestamp 
  - week: week of the timestamp 
  - weekday: weekday of the timestamp 

- movie_for_search : For search index
  - tmdb_id: tmdb's movie ID
  - title: movie tile
  - keyword: keyword which related to the movie defined in TMDB
  - tagline: movie's tagline
  - overview: movie's overview


## ER Diagram

This is a ER diagram of raw dataset and result data.
In TMDB, the data is provided by json format with many fields.
The diagram depicts only a few items for simplicity.

### Input data

![RawData](./imgs/ER_RawData.png)

### Output data

![ResultData](./imgs/ER_ResultData.png)


## references

[^tmdb]: https://www.themoviedb.org
[^imdb]: https://www.imdb.com
[^movielens]: https://grouplens.org/datasets/movielens/
[^tmbdex]: https://developers.themoviedb.org/3/getting-started/daily-file-exports
[^spark]: https://spark.apache.org
[^emr]: https://docs.aws.amazon.com/ja_jp/emr/latest/ManagementGuide/emr-overview.html
[^parquet]: https://parquet.apache.org
[^airflow_setup]:https://airflow.apache.org/installation.html
[^s3spec]: https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html
[^jupyhub]: https://jupyterhub.readthedocs.io/en/stable/