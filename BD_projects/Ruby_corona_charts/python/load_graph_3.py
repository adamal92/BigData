import pandas
import sys
from datetime import datetime, timedelta
import json
import logging
import os
import time
import winsound
from typing import List, Dict, Any, Union

import hdfs
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


def crawl_corona():  # streaming?
    # TODO: catch if there is no internet connection
    response: dict = requests.get(Constants.URL_GOV).json()

    if Constants.SAVE_TO_HDFS:
        save_df_to_hdfs(spark.createDataFrame(data=response["result"]["records"]).toPandas())
        to_spark()
    else:
        to_spark_direct_upside_down(cities=response["result"]["records"])
    # citiesDF: DataFrame = spark.createDataFrame(data=cities)
    # process_data(data_frame=citiesDF)


def save_df_to_hdfs(df: pandas.DataFrame):
    from Hadoop.hdfs import HDFS_handler
    from testsAndOthers.data_types_and_structures import DataTypesHandler
    HDFS_handler.start()
    HDFS_handler.safemode_off()
    time.sleep(5)

    # hdfs_client: hdfs.client.Client = hdfs.client.Client(HDFS_handler.DEFAULT_CLUSTER_PATH)
    hdfs_path: str = f"{HDFS_handler.DEFAULT_CLUSTER_WEB_URL}/{Constants.JSON_PATH}"
    print(requests.put(url=f'http://localhost:9870/webhdfs/v1{HDFS_handler.HADOOP_USER}{Constants.JSON_PATH}?op=MKDIRS', data=df.to_dict()).text)
    # print(requests.get(url=hdfs_path).text)
    print(requests.get(url=f'http://localhost:9870/webhdfs/v1{HDFS_handler.HADOOP_USER}{Constants.JSON_PATH}?op=LISTSTATUS').text)
    # DataTypesHandler.print_data_recursively(data=requests.get(url=hdfs_path).json())
    # from hdfs.ext.dataframe import write_dataframe
    # hdfs.config.catch(Exception)
    # write_dataframe(client=hdfs_client, hdfs_path=hdfs_path, df=df)
    # from hdfs.ext.dataframe import read_dataframe
    # print(read_dataframe(hdfs_client, hdfs_path))

    HDFS_handler.stop()


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

    def get_cities(fullDF: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Get the received DataFrame filtered by latest date
        :param fullDF:
        :return:
        """
        from datetime import datetime, timedelta
        result_length: int = 0
        day: datetime = datetime.now()  # today
        while result_length == 0:
            day_str: str = datetime.strftime(day, '%Y-%m-%d')
            filteredDF: DataFrame = fullDF.filter(fullDF.Date >= day_str)
            filteredDF.cache()

            schema = filteredDF.columns

            final_result: List[Dict] = filteredDF.collect()

            logging.debug(f'{day_str} {type(day_str)}')
            logging.debug("Empty RDD: %s" % (final_result.__len__() == 0))
            day = day - timedelta(1)  # timedelta() indicates how many days ago
            result_length = final_result.__len__()

        filteredDF.show()
        filteredDF.describe().show()
        logging.debug(schema)
        return filteredDF

    filteredDF_ext: DataFrame = get_cities(fullDF=citiesDF)
    filteredDF_ext.cache()
    cities_list: List[Row[str]] = filteredDF_ext.select(filteredDF_ext.City_Name).collect()
    citiesDF = citiesDF.select(
        citiesDF.Date, citiesDF.Cumulative_verified_cases, citiesDF.City_Name,
        citiesDF.Cumulated_recovered, citiesDF["Cumulated_deaths"]
    )

    columns_list: list = [
        "Cumulative_verified_cases", "Cumulated_recovered", "Cumulated_deaths"
    ]
    all_graphs = {}
    for col in columns_list:
        col_dict = {}
        for city_name in cities_list[:3]:
            # print(city_name, type(city_name))
            logging.debug(city_name.City_Name)
            # citiesDF.show()
            cities_newDF: DataFrame = citiesDF.filter(citiesDF.City_Name == city_name.City_Name) \
                .filter(citiesDF[col] != "<15")
            cities_newDF.cache()

            # citiesDF.show()
            # print(citiesDF.toPandas().to_dict())
            from datetime import datetime, timedelta

            # Last_Update: str = datetime.strftime(datetime.now(), '%Y-%m-%d')
            Last_Update: str = cities_newDF.toPandas().to_dict()["Date"].pop(cities_newDF.count()-1)
            # Last_Update: str = cities_newDF.toPandas().to_dict()["Date"].pop(0)
            City: str = city_name.City_Name

            cities_newDF = cities_newDF.drop("City_Name")
            # all_graphs[city_name.City_Name] = {
            #     f"חולים לפי תאריך ב{city_name.City_Name}": {
            #         "data": cities_newDF.toPandas().to_dict(),
            #         "Last_Update": Last_Update,
            #         "City": City
            #     }
            # }
            col_dict[city_name.City_Name] = {
                    "data": cities_newDF.toPandas().to_dict(),
                    "Last_Update": Last_Update,
                    "City": City
            }

            # cities_newDF.show()
            del cities_newDF
        all_graphs[col] = col_dict

    Constants.db.update(
        {
            "graphs_2": {
                "חולים לפי תאריך": all_graphs
            }
        }
    )

    logging.debug(f"spark total time: {time.time() - st} seconds")
    return


def process_data(data_frame: DataFrame) -> Dict:
    st = time.time()

    # spark_session = SparkSession \
    #     .builder \
    #     .enableHiveSupport() \
    #     .getOrCreate()

    # data_frame: List[Dict[str, str, str, str, str, str, str, str, str, int]] = dict_root["result"]["records"]
    data_frame.cache()
    # data_frame.printSchema()
    # data_frame.show(truncate=False)

    # data_frame.filter(
    #     data_frame.City_Name == "תל אביב - יפו" and not data_frame.Cumulative_verified_cases == "<15"
    # ).show()

    # data_frame.filter(
    #     data_frame.City_Name == "תל אביב - יפו" & str(data_frame.Cumulative_verified_cases).isdigit()
    # ).show()
    data_frame = data_frame.select(data_frame.Date, data_frame.Cumulative_verified_cases, data_frame.City_Name)
    data_frame = data_frame.filter(data_frame.City_Name == "תל אביב - יפו") \
        .filter(data_frame.Cumulative_verified_cases != "<15")

    # data_frame.show()
    # print(data_frame.toPandas().to_dict())
    from datetime import datetime, timedelta

    # Last_Update: str = datetime.strftime(datetime.now(), '%Y-%m-%d')
    print(data_frame.count(), data_frame.toPandas().to_dict()["Date"])
    Last_Update: str = data_frame.toPandas().to_dict()["Date"].pop(data_frame.count()-1)
    City: str = data_frame.toPandas().to_dict()["City_Name"].pop(0)

    data_frame = data_frame.drop("City_Name")

    data_frame.show()

    # Constants.db.update(
    #     {
    #         "graphs": {
    #             "חולים לפי תאריך בתל אביב": {
    #                 "data": data_frame.toPandas().to_dict(),
    #                 "Last_Update": Last_Update,
    #                 "City": City
    #
    #                 # "Last_Update": datetime.strftime(datetime.now(), '%Y-%m-%d'),
    #                 # "City": data_frame.toPandas().to_dict()["City_Name"].pop(0)
    #             }
    #         }
    #     }
    # )
    logging.debug(f"spark total time: {time.time() - st} seconds")

    return data_frame.toPandas().to_dict()


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

    # from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    # DataTypesHandler.print_data_recursively(data=json_ret_processed, print_dict=PrintForm.PRINT_DICT.value)
    
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
            crawl_corona()
    finally:
        winsound.MessageBeep(winsound.MB_ICONHAND)
        logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
