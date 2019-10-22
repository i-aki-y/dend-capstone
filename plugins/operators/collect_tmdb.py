import logging
import time
from datetime import datetime

import requests
import psycopg2
import pandas as pd

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CollectTmdbOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 id_file="",
                 conn_config=None,
                 tmdb_api_key="",
                 *args, **kwargs):

        super(CollectTmdbOperator, self).__init__(*args, **kwargs)
        self.id_file = id_file
        self.conn_config = conn_config
        self.tmdb_api_key = tmdb_api_key

    @staticmethod
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
        my_url = "https://api.themoviedb.org/3/movie/{}?api_key={}&append_to_response=reviews,keywords".format(
            movie_id, api_key)
        tm_stamp = datetime.now()
        req = requests.get(my_url)

        return {
            "status": req.status_code,
            "tmdb_id": movie_id,
            "timestamp": tm_stamp,
            "content": req.content.decode() if req.status_code == 200 else None,
        }

    @staticmethod
    def save_result(conn, table_name, status_code, movie_id, timestamp, contents):
        """Save api resoponse to the database
        conn: Connection object of the database
        table_name: Table name to store the result
        movie_id: TMDB's movie id
        timestamp: The timestamp when the api is requested
        contents: Result of API request
        """
        try:
            smt = "INSERT INTO {} (tmdb_id, http_status, ts, content) VALUES (%s, %s, %s, %s)".format(table_name)
            vals = (movie_id, status_code, timestamp, contents)
            conn.cursor().execute(smt, vals)
        except Exception as e:
            logging.exception("%s", e)
            raise ValueError("", e)

    def execute(self, context):
        df = pd.read_csv(self.id_file, names=["id"])
        logging.info("got %s new movies", len(df))
        try:
            # PostgreSQLに接続する
            conn = psycopg2.connect(**self.conn_config)
            conn.set_session(autocommit=True)
            table_name = 'themoviedb'
            cnt = 0
            for tmdb_id in df["id"].unique():
                logging.debug("get tmdb_id: %s", tmdb_id)
                res = CollectTmdbOperator.get_movie_data(int(tmdb_id), self.tmdb_api_key)
                logging.debug("get status: %s", res.get("status", ""))
                CollectTmdbOperator.save_result(conn, table_name,
                                                res["status"], res["tmdb_id"], res["timestamp"], res["content"])
                cnt += 1
                if cnt % 1000 == 0:
                    logging.info("collected %s movies", cnt)

        except Exception as e:
            logging.exception("%s", e)
            raise ValueError("", e)
        finally:
            conn.close()
