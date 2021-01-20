from datetime import datetime, timedelta
import json
import logging
import os
import time
import winsound
from typing import List, Dict, Any, Union

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


class Constants:
    db = {}
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'
    SCHEDULER: BlockingScheduler = BlockingScheduler()
    SAVE_TO_HDFS : bool = False
    RUN_SCHEDULER: bool = False
    JSON_PATH = "graph_json.json"


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


def save_json_to_hdfs(filename: str, file_path: str):
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.start()

    HDFS_handler.delete_file(filename=filename)
    HDFS_handler.create_file(file_path=file_path)
    time.sleep(2)
    HDFS_handler.list_files()

    HDFS_handler.stop()


def crawl_corona_batch():
    response: dict = requests.get(Constants.URL_GOV).json()
    try:
        os.mkdir(f"json/{Constants.JSON_PATH}")
    except FileExistsError:
        logging.debug("file exists")
    time.sleep(1)
    with open(f"json/{Constants.JSON_PATH}", 'w') as outfile:
        json.dump(response, outfile)
        save_json_to_hdfs(filename=outfile.name, file_path=os.path.abspath(outfile.name))

    # citiesDF: DataFrame = spark.createDataFrame(data=cities)
    # process_data(data_frame=citiesDF)


def crawl_corona():  # streaming?
    # TODO: catch if there is no internet connection
    response: dict = requests.get(Constants.URL_GOV).json()
    to_spark_direct_upside_down(cities=response["result"]["records"])
    
    # citiesDF: DataFrame = spark.createDataFrame(data=cities)
    # process_data(data_frame=citiesDF)


def to_spark_direct_upside_down(cities: dict):
    st = time.time()

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    # cities: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.cache()
    # citiesDF.printSchema()
    # citiesDF.show(truncate=False)

    # citiesDF.filter(
    #     citiesDF.City_Name == "תל אביב - יפו" and not citiesDF.Cumulative_verified_cases == "<15"
    # ).show()

    # citiesDF.filter(
    #     citiesDF.City_Name == "תל אביב - יפו" & str(citiesDF.Cumulative_verified_cases).isdigit()
    # ).show()
    citiesDF = citiesDF.select(citiesDF.Date, citiesDF.Cumulative_verified_cases, citiesDF.City_Name)
    citiesDF = citiesDF.filter(citiesDF.City_Name == "תל אביב - יפו") \
        .filter(citiesDF.Cumulative_verified_cases != "<15")

    # citiesDF.show()
    # print(citiesDF.toPandas().to_dict())
    from datetime import datetime, timedelta

    # Last_Update: str = datetime.strftime(datetime.now(), '%Y-%m-%d')
    Last_Update: str = citiesDF.toPandas().to_dict()["Date"].pop(citiesDF.count()-1)
    City: str = citiesDF.toPandas().to_dict()["City_Name"].pop(0)

    citiesDF = citiesDF.drop("City_Name")

    citiesDF.show()

    Constants.db.update(
        {
            "graphs": {
                "חולים לפי תאריך בתל אביב": {
                    "data": citiesDF.toPandas().to_dict(),
                    "Last_Update": Last_Update,
                    "City": City

                    # "Last_Update": datetime.strftime(datetime.now(), '%Y-%m-%d'),
                    # "City": citiesDF.toPandas().to_dict()["City_Name"].pop(0)
                }
            }
        }
    )
    logging.debug(f"spark total time: {time.time() - st} seconds")

    return


def process_data(data_frame: DataFrame) -> Dict:
    st = time.time()

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    # cities: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.cache()
    # citiesDF.printSchema()
    # citiesDF.show(truncate=False)

    # citiesDF.filter(
    #     citiesDF.City_Name == "תל אביב - יפו" and not citiesDF.Cumulative_verified_cases == "<15"
    # ).show()

    # citiesDF.filter(
    #     citiesDF.City_Name == "תל אביב - יפו" & str(citiesDF.Cumulative_verified_cases).isdigit()
    # ).show()
    citiesDF = citiesDF.select(citiesDF.Date, citiesDF.Cumulative_verified_cases, citiesDF.City_Name)
    citiesDF = citiesDF.filter(citiesDF.City_Name == "תל אביב - יפו") \
        .filter(citiesDF.Cumulative_verified_cases != "<15")

    # citiesDF.show()
    # print(citiesDF.toPandas().to_dict())
    from datetime import datetime, timedelta

    # Last_Update: str = datetime.strftime(datetime.now(), '%Y-%m-%d')
    print(citiesDF.count(), citiesDF.toPandas().to_dict()["Date"])
    Last_Update: str = citiesDF.toPandas().to_dict()["Date"].pop(citiesDF.count()-1)
    City: str = citiesDF.toPandas().to_dict()["City_Name"].pop(0)

    citiesDF = citiesDF.drop("City_Name")

    citiesDF.show()

    Constants.db.update(
        {
            "graphs": {
                "חולים לפי תאריך בתל אביב": {
                    "data": citiesDF.toPandas().to_dict(),
                    "Last_Update": Last_Update,
                    "City": City

                    # "Last_Update": datetime.strftime(datetime.now(), '%Y-%m-%d'),
                    # "City": citiesDF.toPandas().to_dict()["City_Name"].pop(0)
                }
            }
        }
    )
    logging.debug(f"spark total time: {time.time() - st} seconds")

    return


def to_spark():
    st = time.time()

    from Hadoop.hdfs import HDFS_handler
    from Spark.Spark_handler_class import Spark_handler

    HDFS_handler.start()
    HDFS_handler.safemode_off()

    json_ret_processed: dict = Spark_handler.pass_to_spark(
        file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}{HDFS_handler.HADOOP_USER}/{Constants.JSON_PATH}",
        process_fn=process_data
    )

    HDFS_handler.safemode_on()
    HDFS_handler.stop()

    from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    DataTypesHandler.print_data_recursively(data=json_ret_processed, print_dict=PrintForm.PRINT_DICT.value)
    
    # Constants.db.update(json_ret_processed)

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
        firebase_config()

        if Constants.RUN_SCHEDULER:
            Constants.SCHEDULER.start()
        else:
            if Constants.SAVE_TO_HDFS:
                crawl_corona_batch()
                to_spark()
            else:
                crawl_corona()
    finally:
        winsound.MessageBeep(winsound.MB_ICONHAND)
        logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
