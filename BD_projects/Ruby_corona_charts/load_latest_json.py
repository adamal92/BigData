import datetime
import json
import logging
import time
import winsound
from typing import List, Dict

import requests as requests
from firebase import Firebase

from apscheduler.schedulers.blocking import BlockingScheduler
# https://stackoverflow.com/questions/22715086/scheduling-python-script-to-run-every-hour-accurately
# https://data.gov.il/dataset/covid-19/resource/0995c344-6a7a-4557-99ff-28ee6f3149b3
# https://data.gov.il/dataset/covid-19/resource/89f61e3a-4866-4bbf-bcc1-9734e5fee58e
# https://console.firebase.google.com/u/2/project/corona-charts-33e8a/database/corona-charts-33e8a-default-rtdb/data/~2F
# https://stackoverflow.com/questions/30483977/python-get-yesterdays-date-as-a-string-in-yyyy-mm-dd-format/30484112
from pyspark import RDD
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame


class Constants:
    db = {}
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'
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
    response: dict = requests.get(Constants.URL_GOV).json()
    to_spark_direct(cities=response["result"]["records"])
    # logging.debug(response["result"]["records"])

    # Constants.db.update({"cities": Constants.cities})
    # Constants.db.update({"cities": unique})


def to_spark_direct(cities: dict):
    st = time.time()

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    # cities: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.printSchema()
    citiesDF.show(truncate=False)
    # from datetime import date
    # today = date.today()
    # print("Today's date:", today)
    # print(type(citiesDF.filter(citiesDF.Date >= date.today())))
    # filteresDF: DataFrame = citiesDF.filter(citiesDF.Date >= "2021-01-09")
    from datetime import datetime, timedelta
    result_length: int = 0
    day: datetime = datetime.now()  # today
    while result_length == 0:
        # print(type(day))
        day_str: str = datetime.strftime(day, '%Y-%m-%d')
        filteresDF: DataFrame = citiesDF.filter(citiesDF.Date >= day_str)
        filteresDF.show()
        # print(type(filteresDF.collect()))
        final_result: List[Dict] = filteresDF.collect()
        # print(filteresDF.select("*"))
        # print(datetime.strftime(day, '%Y-%m-%d'), type(datetime.strftime(day, '%Y-%m-%d')))
        print(day_str, type(day_str))
        print("Empty RDD: %s" % (final_result.__len__() == 0))
        day = day - timedelta(1)  # timedelta() indicates how many days ago
        result_length = final_result.__len__()

    Constants.db.update({"cities_3": final_result})  # load to firebase

    # from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    # DataTypesHandler.print_data_recursively(data=json_count_names, print_dict=PrintForm.PRINT_DICT.value)

    logging.debug(f"spark total time: {time.time() - st} seconds")


def to_spark():
    st = time.time()

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    df: DataFrame = spark_session.read.json("json/city_json.json")
    df.show()

    df.createOrReplaceTempView("table_1")

    df2: DataFrame = spark.sql("SELECT result from table_1")

    df2.show()

    rdd: RDD = df2.toJSON()
    print(rdd.count())

    first_elem: str = rdd.first()

    print(first_elem[0:5000:1], len(first_elem))
    dict_root: dict = json.loads(first_elem)
    print()
    print(dict_root["result"].keys())
    print(dict_root["result"]["records"].pop())
    cities: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.printSchema()
    citiesDF.show(truncate=False)
    from datetime import date
    today = date.today()
    print("Today's date:", today)
    print(type(citiesDF.filter(citiesDF.Date >= date.today())))
    filteresDF: DataFrame = citiesDF.filter(citiesDF.Date >= "2021-01-09")
    filteresDF.show()
    # print(type(filteresDF.collect()))
    final_result: List[Dict] = filteresDF.collect()

    Constants.db.update({"cities_2": final_result})  # load to firebase

    # from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    # DataTypesHandler.print_data_recursively(data=json_count_names, print_dict=PrintForm.PRINT_DICT.value)

    logging.debug(f"spark total time: {time.time() - st} seconds")


@Constants.SCHEDULER.scheduled_job('cron', day_of_week='mon-sun', hour=14, minute=15, second=30)
def scheduled_job():
    print('This job is run every weekday at 0am.')
    firebase_config()
    crawl_corona()
    to_spark()
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
        # to_spark()

    finally:
        winsound.MessageBeep(winsound.MB_ICONHAND)
        logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
