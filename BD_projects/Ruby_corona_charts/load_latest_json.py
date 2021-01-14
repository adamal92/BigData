import datetime
import json
import logging
import time
import winsound
from typing import List, Dict, Any, Union

import requests as requests
from firebase import Firebase

from apscheduler.schedulers.blocking import BlockingScheduler
# https://stackoverflow.com/questions/22715086/scheduling-python-script-to-run-every-hour-accurately
# https://data.gov.il/dataset/covid-19/resource/0995c344-6a7a-4557-99ff-28ee6f3149b3
# https://data.gov.il/dataset/covid-19/resource/89f61e3a-4866-4bbf-bcc1-9734e5fee58e
# https://console.firebase.google.com/u/2/project/corona-charts-33e8a/database/corona-charts-33e8a-default-rtdb/data/~2F
# https://stackoverflow.com/questions/30483977/python-get-yesterdays-date-as-a-string-in-yyyy-mm-dd-format/30484112
from pyspark import RDD, Row
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import explode, create_map


class Constants:
    db = {}
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'
    SCHEDULER: BlockingScheduler = BlockingScheduler()
    json_rows = []


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
    # print(response["result"]["records"].pop().items())
    # return
    to_spark_direct_upside_down(cities=response["result"]["records"])
    # my_dict2 = {y: x for x, y in filteresDF.toPandas().to_dict().items()}

    # logging.debug(response["result"]["records"])

    # Constants.db.update({"cities": Constants.cities})
    # Constants.db.update({"cities": unique})


def to_spark_direct_upside_down(cities: dict):
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
        filteresDF.describe().show()
        schema = filteresDF.columns
        logging.debug(schema)
        # print(type(filteresDF.collect()))
        final_result: List[Dict] = filteresDF.collect()
        # print(filteresDF.select("*"))
        # print(datetime.strftime(day, '%Y-%m-%d'), type(datetime.strftime(day, '%Y-%m-%d')))
        logging.debug(f'{day_str} {type(day_str)}')
        logging.debug("Empty RDD: %s" % (final_result.__len__() == 0))
        day = day - timedelta(1)  # timedelta() indicates how many days ago
        result_length = final_result.__len__()
    # print(type(filteresDF.rdd.mapValues(lambda city: (city["City_Name"], city["City_Code"]))))
    # print(filteresDF.rdd.mapValues(lambda city: (city["City_Name"], city["City_Code"])).collectAsMap())
    # print(filteresDF.select(explode(filteresDF.rdd.collectAsMap())))
    # print(explode(filteresDF))
    # print(filteresDF.toPandas().to_dict())
    # my_dict2 = {y: x for x, y in filteresDF.toPandas().to_dict().items()}
    # print(my_dict2)
    # rdd: pyspark.rdd.RDD = sc.parallelize(list)
    def append_json(row: Row):
        # for item in row.asDict(recursive=True).items():
        my_dict2 = {y: x for x, y in row.asDict(recursive=True).items()}
        # print(my_dict2)
        # print(row.asDict(recursive=True))
        return {row["City_Name"]: row.asDict()} # {"counter": row.asDict()}
        # Constants.json_rows.append({
        #     "City_Code": row["City_Code"],
        #     "City_Name": row.City_Code,
        #     "Cumulated_deaths": row[2]
        # })
    filteresDF.foreach(append_json)
    df55: DataFrame = spark.createDataFrame(data=filteresDF.rdd.map(append_json).collect())
    df55.show()
    print(df55.toPandas().to_dict())
    # print(Constants.json_rows)
    # filteresDF.show()
    # filteresDF.foreachPartition(lambda x: print(x))
   #  metric: Column = create_map(filteresDF.columns)
   #  metric: Column = create_map([
   #      filteresDF.City_Name,
   #      [
   #          filteresDF.City_Code,
   #          filteresDF.Cumulated_deaths,
   #          filteresDF.Cumulated_number_of_diagnostic_tests,
   #          filteresDF.Cumulated_number_of_tests,
   #          filteresDF.Cumulated_recovered,
   #          filteresDF.Cumulated_vaccinated,
   #          filteresDF["Cumulative_verified_cases"],
   #          filteresDF.Date,
   #          filteresDF["_id"]
   #      ]
   #  ])
   #  print(filteresDF.select(explode(metric)))
   #  filteresDF.select(explode(metric)).show()
   #  filteresDF.select(create_map(filteresDF.columns).alias("map")).show()

    # Constants.db.update(
    #     {
    #         "cities_3": {
    #             "schema": schema,
    #             "data": final_result,
    #             "filteresDF": filteresDF.toJSON().collect(),
    #             # "ok_3": json.loads(str(dict({"data": filteresDF.toJSON().collect()}))),
    #             # "ok_5": json.load(filteresDF.toJSON().collect()),
    #             # "ok": filteresDF.toJSON().keys(),
    #             # "ok_2": filteresDF.toJSON().collectAsMap(),
    #             "ok": filteresDF.toPandas().to_dict(),
    #             "shit": df55.toPandas().to_dict()
    #         }
    #     }
    # )  # load to firebase

    Constants.db.update(
        {"cities_final": df55.toPandas().to_dict()}
    )  # load to firebase

    # from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    # DataTypesHandler.print_data_recursively(data=json_count_names, print_dict=PrintForm.PRINT_DICT.value)

    logging.debug(f"spark total time: {time.time() - st} seconds")


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
        filteresDF.describe().show()
        schema = filteresDF.columns
        logging.debug(schema)
        # print(type(filteresDF.collect()))
        final_result: List[Dict] = filteresDF.collect()
        # print(filteresDF.select("*"))
        # print(datetime.strftime(day, '%Y-%m-%d'), type(datetime.strftime(day, '%Y-%m-%d')))
        logging.debug(f'{day_str} {type(day_str)}')
        logging.debug("Empty RDD: %s" % (final_result.__len__() == 0))
        day = day - timedelta(1)  # timedelta() indicates how many days ago
        result_length = final_result.__len__()
    # print(type(filteresDF.rdd.mapValues(lambda city: (city["City_Name"], city["City_Code"]))))
    # print(filteresDF.rdd.mapValues(lambda city: (city["City_Name"], city["City_Code"])).collectAsMap())
    # print(filteresDF.select(explode(filteresDF.rdd.collectAsMap())))
    # print(explode(filteresDF))
    # print(filteresDF.toPandas().to_dict())
    my_dict2 = {y: x for x, y in filteresDF.toPandas().to_dict().items()}
    print(my_dict2)
   #  metric: Column = create_map(filteresDF.columns)
   #  metric: Column = create_map([
   #      filteresDF.City_Name,
   #      [
   #          filteresDF.City_Code,
   #          filteresDF.Cumulated_deaths,
   #          filteresDF.Cumulated_number_of_diagnostic_tests,
   #          filteresDF.Cumulated_number_of_tests,
   #          filteresDF.Cumulated_recovered,
   #          filteresDF.Cumulated_vaccinated,
   #          filteresDF["Cumulative_verified_cases"],
   #          filteresDF.Date,
   #          filteresDF["_id"]
   #      ]
   #  ])
   #  print(filteresDF.select(explode(metric)))
   #  filteresDF.select(explode(metric)).show()
   #  filteresDF.select(create_map(filteresDF.columns).alias("map")).show()

    Constants.db.update(
        {
            "cities_3": {
                "schema": schema,
                "data": final_result,
                "filteresDF": filteresDF.toJSON().collect(),
                # "ok_3": json.loads(str(dict({"data": filteresDF.toJSON().collect()}))),
                # "ok_5": json.load(filteresDF.toJSON().collect()),
                # "ok": filteresDF.toJSON().keys(),
                # "ok_2": filteresDF.toJSON().collectAsMap(),
                "ok": filteresDF.toPandas().to_dict()
            }
        }
    )  # load to firebase

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


@Constants.SCHEDULER.scheduled_job('cron', day_of_week='mon-sun', hour=7, minute=0, second=0)
def scheduled_job():
    print('This job is run every weekday at 7am.')
    firebase_config()
    crawl_corona()
    # to_spark()
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
