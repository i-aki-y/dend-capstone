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

PHTHON=python


# download daily export file
# cf.https://developers.themoviedb.org/3/getting-started/daily-file-exports
MOVIE_FILE=movie_ids_08_20_2019.json
curl -O http://files.tmdb.org/p/exports/${MOVIE_FILE}.gz

# unzip downloaded file
ungzip ${MOVIE_FILE}.gz

# extract movie id
cat ${MOVIE_FILE} | jq .id > ids.csv

# collect movie data from id list
${PHTHON} collect_movie_data.py ids.csv

# upload collected data to s3
${PHTHON} upload_tmdb.py
