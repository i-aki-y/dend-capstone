# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime
import urllib.parse as urlparse
import argparse

import requests
import psycopg2
import pandas as pd

from logging import basicConfig, getLogger, DEBUG

basicConfig(level=DEBUG)
logger = getLogger(__name__)


API_KEY = os.environ["TMDB_API_KEY"]
LOCAL_DATA_PATH = os.environ["LOCAL_DATA_PATH"]

def get_pg_config():
    """Get postgres configuration
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': '',
            'host': 'localhost',
            'port': '5432',
        }

    logger.info("Use DATABASE_URL")
    url = urlparse.urlparse(db_url)
    return {
        "dbname": url.path[1:],
        "user": url.username,
        "password": url.password,
        "host": url.hostname,
        "port": url.port,
    }


def init_db(conn_config):
    """Initialize database
    This create `themoviedb` table in the database
    """
    try:
        # PostgreSQLに接続する
        conn = psycopg2.connect(**conn_config)
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        table_name = 'themoviedb'
        logger.info("Init table %s", table_name)
        # create sparkify database with UTF8 encoding
        cur.execute("DROP TABLE IF EXISTS {}".format(table_name))
        cur.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        tmdb_id integer,
        http_status integer,
        ts timestamp,
        content json
        )
        '''.format(table_name))
    except Exception as e:
        logger.exception("%s", e)
    finally:
        conn.close()


def get_movie_data(movie_id, api_key, delay_sec=0.35):
    """Get movie data by useing TMDB API
    movie_id: TMDB's movie id
    api_key: TMDB's api key
    delay_sec: delay time in second for each request. This should be larger than 0.3

    Return: dict of status code, movie_id, timestamp, response body
    """
    if delay_sec < 0.3:
        raise ValueError("delay_sec should be larger than 0.3")
    time.sleep(delay_sec)
    my_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&append_to_response=reviews,keywords"
    ts = datetime.now()
    r = requests.get(my_url)

    return {
        "status": r.status_code,
        "tmdb_id": movie_id,
        "timestamp": ts,
        "content": r.content.decode() if r.status_code == 200 else None,
    }


def save_result(conn, table_name, status_code, movie_id, timestamp, contents):
    """Save api resoponse to the database
    conn: Connection object of the database
    table_name: Table name to store the result
    movie_id: TMDB's movie id
    timestamp: The timestamp when the api is requested
    contents: Result of API request
    """
    try:
        print("save_result", file=sys.stderr)
        smt = "INSERT INTO {} (tmdb_id, http_status, ts, content) VALUES (%s, %s, %s, %s)".format(table_name)
        vals = (movie_id, status_code, timestamp, contents)
        conn.cursor().execute(smt, vals)
    except Exception as e:
        logger.exception("%s", e)


def main(id_file):
    """Initialize database and request movie data to the tmdb for each movie ids
    """
    conn_config = get_pg_config()
    init_db(conn_config)
    df = pd.read_csv(id_file, names=["id"])
    try:
        # PostgreSQLに接続する
        conn = psycopg2.connect(**conn_config)
        conn.set_session(autocommit=True)
        table_name = 'themoviedb'
        for tmdb_id in df["id"].unique():
            logger.debug("get tmdb_id: %s", tmdb_id)
            res = get_movie_data(int(tmdb_id), API_KEY)
            logger.debug("get status: %s", res.get("status", ""))
            save_result(conn, table_name, res["status"], res["tmdb_id"], res["timestamp"], res["content"])

    except Exception as e:
        logger.exception("%s", e)
    finally:
        conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Collect tmdb movie data')

    parser.add_argument('movie_id_file', help='File path which contains tmdb movie ids. Each row of the file contains id number only')
    args = parser.parse_args()

    main(args.movie_id_file)
