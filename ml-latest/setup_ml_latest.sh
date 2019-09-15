#! /bin/sh

#### The following environment variable should be defined or exported in elsewhere

#TMDB_API_KEY=
#AWS_ACCESS_KEY=
#AWS_SECRET_ACCESS_KEY=

##s3 bucket use for this project
#PROJ_BUCKET=

## s3 path where store tmdb raw data
#TMDB_DATA_PATH=

## s3 path where store MovieLens raw data
#ML_DATA_PATH=

######

curl -O http://files.grouplens.org/datasets/movielens/ml-latest.zip
unzip ml-latest.zip

aws s3 cp ./ml-latest/movies.csv s3://${PROJ_BUCKET}/${ML_DATA_PATH}/
aws s3 cp ./ml-latest/links.csv s3://${PROJ_BUCKET}/${ML_DATA_PATH}/
aws s3 cp ./ml-latest/ratings.csv s3://${PROJ_BUCKET}/${ML_DATA_PATH}/
aws s3 cp ./ml-latest/tags.csv s3://${PROJ_BUCKET}/${ML_DATA_PATH}/
