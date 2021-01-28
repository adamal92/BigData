import datetime
import glob
import json
import logging
import os
import time

from typing import List, Dict, Any, Union

import pyspark
import requests as requests
from firebase import Firebase

from apscheduler.schedulers.blocking import BlockingScheduler
# https://stackoverflow.com/questions/22715086/scheduling-python-script-to-run-every-hour-accurately
# https://data.gov.il/dataset/covid-19  # מאגר קורונה
# https://data.gov.il/dataset/covid-19/resource/8a21d39d-91e3-40db-aca1-f73f7ab1df69  # טבלת יישובים
# https://data.gov.il/dataset/covid-19/resource/0995c344-6a7a-4557-99ff-28ee6f3149b3  # טבלת יישובים README
# https://data.gov.il/dataset/covid-19/resource/89f61e3a-4866-4bbf-bcc1-9734e5fee58e  # קבוצות מין וגיל
# https://console.firebase.google.com/u/2/project/corona-charts-33e8a/database/corona-charts-33e8a-default-rtdb/data/~2F
# https://stackoverflow.com/questions/30483977/python-get-yesterdays-date-as-a-string-in-yyyy-mm-dd-format/30484112
from pyspark import RDD, Row, Accumulator
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import explode, create_map


# https://www.cbs.gov.il/he/Pages/%D7%A1%D7%93%D7%A8%D7%95%D7%AA-
# %D7%A2%D7%99%D7%AA%D7%99%D7%95%D7%AA-%D7%91%D7%90%D7%9E%D7%A6%D7%A2%D7%95%D7%AA-API.aspx
class Constants:
    db = {}
    URL_GREEN_RED = 'https://data.gov.il/api/3/action/datastore_search?' \
                    'resource_id=f1d13bbd-4f84-4cde-82ed-e075c942de12&limit=100000'  # 167
    SCHEDULER: BlockingScheduler = BlockingScheduler()


def firebase_config():
    config = {
        # "apiKey": "apiKey",
        # "authDomain": "projectId.firebaseapp.com",
        # "databaseURL": "https://databaseName.firebaseio.com",
        # "storageBucket": "projectId.appspot.com",
        # "serviceAccount": "path/to/serviceAccountCredentials.json"  # (optional)

        "apiKey": "AIzaSyCOt619fNqEuIgFpzf20h2cmC6tFeQYuTE",
        "authDomain": "corona-charts-33e8a.firebaseapp.com",
        "databaseURL": "https://corona-charts-33e8a-default-rtdb.firebaseio.com/",
        "storageBucket": "corona-charts-33e8a.appspot.com"
    }

    firebase = Firebase(config)

    Constants.db = firebase.database()


def crawl_corona():
    # TODO: catch if there is no internet connection
    response: dict = requests.get(Constants.URL_GREEN_RED).json()
    to_spark_direct_upside_down(countries=response["result"]["records"])


def to_spark_direct_upside_down(countries: dict):
    st = time.time()

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .config('spark.sql.debug.maxToStringFields', 2000) \
        .getOrCreate()

    # cities: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    countiesDF: DataFrame = spark.createDataFrame(data=countries)
    countiesDF.cache()  # without: 152.403 s, with: 14.531 s
    # countiesDF.printSchema()
    # countiesDF.show(truncate=False)

    from datetime import datetime, timedelta
    result_length: int = 0
    day: datetime = datetime.now()  # today
    while result_length == 0:
        day_str: str = datetime.strftime(day, '%Y-%m-%d')
        filteredDF: DataFrame = countiesDF.filter(countiesDF.update_date >= day_str)
        filteredDF.cache()

        schema = filteredDF.columns

        final_result: List[Dict] = filteredDF.collect()

        logging.debug(f'{day_str} {type(day_str)}')
        logging.debug("Empty RDD: %s" % (final_result.__len__() == 0))
        day = day - timedelta(1)  # timedelta() indicates how many days ago
        result_length = final_result.__len__()

    # filteredDF.show()
    # filteredDF.describe().show()
    logging.debug(schema)

    # Constants.db.update(
    #     {
    #         "Countries_Red_Green": filteredDF.toPandas().to_dict()
    #     }
    # )

    def append_json(row: Row):
        return {row["destination"]: row.country_status}  # {"counter": row.asDict()}

    # filteredDF.foreach(append_json)
    filteredDF = filteredDF.select("destination", "country_status")
    filteredDF.show()
    cities_final_df: DataFrame = spark.createDataFrame(data=filteredDF.rdd.map(append_json).collect())

    cities_final_df.cache()
    # # cities_final_df.show()
    #
    # from testsAndOthers.data_types_and_structures import DataTypesHandler
    # DataTypesHandler.print_data_recursively(
    #     data=cities_final_df.toPandas().to_dict(), print_dict=DataTypesHandler.PRINT_DICT
    # )

    dicty = {}
    for row in filteredDF.collect():
        dicty[row.destination] = row.country_status

    from data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=dicty, print_dict=DataTypesHandler.PRINT_DICT
    )

    # TODO: red green bug (firebase 167 keys limit)
    # TODO: get whole country status from api
    # TODO: generate graphs for dead, vaccinated, deadliness
    # TODO: get vaccinated data
    # print(pyspark.sql.functions.split(cities_final_df, "פקיסטן"))
    Constants.db.update(
        {
            "Countries_Red_Green_2": {
                "full_table": filteredDF.toPandas().to_dict(),
                "collected": filteredDF.collect(),
                # "dicty": dicty
                # "color_by_country": cities_final_df.toPandas().to_dict()
            }
        }
    )
    return
    # 
    # Constants.db.update(
    #     {
    #         "cities_3": {
    #             "schema": schema,
    #             "data": final_result,
    #             "filteredDF": filteredDF.toJSON().collect(),
    #             "ok": filteredDF.toPandas().to_dict()
    #         }
    #     }
    # )  # load to firebase

    # Constants.db.update(
    #     {"cities_final": cities_final_df.toPandas().to_dict()}
    # )  # load to firebase

    total: Accumulator = spark.sparkContext.accumulator(0)
    less: Accumulator = spark.sparkContext.accumulator(0)
    keys_lst: list = []
    updated_to: str = ''

    def add(row: Row, total: Accumulator, less: Accumulator):
        for val in row.asDict().values():
            if str(val).isdigit():
                total += int(val)
            elif val == "<15":
                less += 1

    for key in filteredDF.toPandas().keys():
        if key == 'Date':
            print(filteredDF.rdd.first().Date)
            updated_to = filteredDF.rdd.first().Date
            continue
        elif key in ["_id", "City_Name", "City_Code"]: continue
        print(key)
        filteredDF.select(filteredDF[key]).foreach(lambda row: add(row, total, less))
        print(total.value)
        print(less.value)
        keys_lst.append(
            {
                str(key): {
                    "total": total.value,
                    "less_<15": less.value
                }
            }
        )
        total.value = 0
        less.value = 0

    # keys_lst.append(updated_to)
    # Constants.db.update(
    #     {
    #         "israel_final_3": keys_lst
    #     }
    # )
    # 
    # Constants.db.update(
    #     {
    #         "israel_final_2": [
    #             keys_lst, updated_to
    #         ]
    #     }
    # )
    # 
    # Constants.db.update(
    #     {
    #         "israel_final": {
    #             "data": keys_lst,
    #             "Last_Update": updated_to
    #         }
    #     }
    # )

    logging.debug(f"spark total time: {time.time() - st} seconds")


@Constants.SCHEDULER.scheduled_job('cron', day_of_week='mon-sun', hour=7, minute=0, second=0)
def scheduled_job():
    print('This job is run every weekday at 7am.')
    firebase_config()
    crawl_corona()
    print(datetime.datetime.now())
    winsound.MessageBeep(winsound.MB_OK)


# TODO: load json using spark & save to hdfs, sqlite, elastic & ml? mining? cluster?
def main():
    st = time.time()

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('flaskwebgui').setLevel(logging.ERROR)
    logging.getLogger('BaseHTTPRequestHandler').setLevel(logging.ERROR)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('my_log').setLevel(logging.DEBUG)

    try:
        # Constants.SCHEDULER.start()
        firebase_config()
        crawl_corona()
    finally:
        logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
