# -*- coding: utf-8 -*-
import os
import argparse
import datetime
import requests
import psycopg2
import pandas as pd

from logging import basicConfig, getLogger, DEBUG

basicConfig(level=DEBUG)
logger = getLogger(__name__)

API_KEY = os.environ["TMDB_API_KEY"]


def get_movie_changes(start_date, page, api_key):
    """Get movie changes by using TMDB API

    See, https://developers.themoviedb.org/3/changes/get-movie-change-list

    start_date: start_date parameter of movie/changes api
    page: page parameter of movie/changes api
    api_key: TMDB's api key

    Return: list of movie ids
    """

    my_url = f"https://api.themoviedb.org/3/movie/changes?api_key={api_key}&start_date={start_date}&page={page}"
    res = requests.get(my_url)
    logger.info("status code %s", res.status_code)
    content = res.json() if res.status_code == 200 else None

    if content is not None:
        ids = [int(item["id"]) for item in content["results"]]
        total_pages = int(content["total_pages"])
        if  total_pages > page:
            logger.info("total_pages %s, cur_page %s", total_pages, page)
            ids += get_movie_changes(start_date, page+1, api_key)
        return ids
    return []


def main(start_date):
    logger.info("request chage at %s", start_date)
    ids = get_movie_changes(start_date, 1, API_KEY)
    logger.info("obtained id counts %s", len(ids))

    pd.DataFrame(ids).to_csv("./change_ids.csv", index=False, header=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get tmdb change id list')

    parser.add_argument('--start_date', help='The date (yyyy-MM-dd) used to request tmdb change api call')
    args = parser.parse_args()

    start_date = datetime.datetime.now().strftime("%Y-%m-%d")
    if args.start_date is not None :
        start_date = args.start_date

    logger.info("set start_date %s", start_date)
    main(start_date)
