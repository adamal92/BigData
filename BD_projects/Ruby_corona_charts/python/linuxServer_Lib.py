import logging
import subprocess
import sys
import time
from subprocess import Popen
from typing import List, Dict

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from pyspark import Row, Accumulator
from pyspark.python.pyspark.shell import spark
from pyspark.sql import DataFrame


class Constants:
    db = {}
    # where: filter by sql query, outFields: filter columns, f: format style
    API_URL = 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/' \
              'ncov_cases2_v1/FeatureServer/2/query?' \
              'where=Country_Region = \'israel\'' \
              '&outFields=Country_Region, Last_Update, Confirmed, Deaths, Recovered, Active, Mortality_Rate' \
              '&outSR=4326' \
              '&f=json' \
        # get all:
    # '&outFields=*' \
    # 'where=1%3D1' \
    URL_GREEN_RED = 'https://data.gov.il/api/3/action/datastore_search?' \
                    'resource_id=f1d13bbd-4f84-4cde-82ed-e075c942de12&limit=100000'  # 167
    SCHEDULER: BlockingScheduler = BlockingScheduler()
    SAVE_TO_HDFS: bool = False
    RUN_SCHEDULER: bool = True
    JSON_PATH = "graph_json.json"
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'


def run_py_file(filename: str):
    process: Popen = subprocess \
        .Popen([sys.executable, f'{os.getcwd()}\\{filename}.py'], stdout=sys.stdout, shell=True,
               creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)  # search
    # .Popen(["python", f'{os.getcwd()}\\start_search.py'], stdout=sys.stdout)  # search
    process.communicate()  # wait for process to end


def crawl_corona_UN():
    response = requests.get(url=Constants.API_URL)
    print(response.text)
    from testsAndOthers.data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(data=response.json(), print_dict=DataTypesHandler.PRINT_DICT)
    print(response.json()["features"][0]["attributes"])
    Constants.db.update(
        {
            "israel_UN_WHO": response.json()["features"][0]["attributes"]
        }
    )
    return


def crawl_corona_red_green():
    # TODO: catch if there is no internet connection
    response: dict = requests.get(Constants.URL_GREEN_RED).json()
    to_spark_direct_upside_down_red_green(countries=response["result"]["records"])


def to_spark_direct_upside_down_red_green(countries: dict):
    countiesDF: DataFrame = spark.createDataFrame(data=countries)
    countiesDF.cache()  # without: 152.403 s, with: 14.531 s

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

    logging.debug(schema)

    Constants.db.update(
         {
             "Countries_Red_Green": filteredDF.toPandas().to_dict()
         }
    )

    def append_json(row: Row[tuple]):
        return {row["destination"]: row.country_status}  # {"counter": row.asDict()}

    filteredDF = filteredDF.select("destination", "country_status")
    filteredDF.show()
    cities_final_df: DataFrame = spark.createDataFrame(data=filteredDF.rdd.map(append_json).collect())

    cities_final_df.cache()

    dicty = {}
    for row in filteredDF.collect():
        dicty[row.destination] = row.country_status

    from testsAndOthers.data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=dicty, print_dict=DataTypesHandler.PRINT_DICT
    )

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


def crawl_corona_israel_final():
    response: dict = requests.get(Constants.URL_GOV).json()
    to_spark_direct_upside_down_israel_final(cities=response["result"]["records"])


def to_spark_direct_upside_down_israel_final(cities: dict):
    st = time.time()

    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.cache()

    from datetime import datetime, timedelta
    result_length: int = 0
    day: datetime = datetime.now()  # today
    while result_length == 0:
        day_str: str = datetime.strftime(day, '%Y-%m-%d')
        filteresDF: DataFrame = citiesDF.filter(citiesDF.Date >= day_str)

        schema = filteresDF.columns

        final_result: List[Dict] = filteresDF.collect()

        logging.debug(f'{day_str} {type(day_str)}')
        logging.debug("Empty RDD: %s" % (final_result.__len__() == 0))
        day = day - timedelta(1)  # timedelta() indicates how many days ago
        result_length = final_result.__len__()

    del citiesDF
    filteresDF.cache()

    def append_json(row: Row):
        return {row["City_Name"]: row.asDict()}  # {"counter": row.asDict()}

    filteresDF.foreach(append_json)
    cities_final_df: DataFrame = spark.createDataFrame(data=filteresDF.rdd.map(append_json).collect())
    # cities_final_df.cache()

    Constants.db.update(
        {"cities_final": cities_final_df.toPandas().to_dict()}
    )  # load to firebase

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

    for key in filteresDF.toPandas().keys():
        if key == 'Date':
            print(filteresDF.rdd.first().Date)
            updated_to = filteresDF.rdd.first().Date
            continue
        elif key in ["_id", "City_Name", "City_Code"]: continue
        print(key)
        filteresDF.select(filteresDF[key]).foreach(lambda row: add(row, total, less))
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

    Constants.db.update(
        {
            "israel_final": {
                "data": keys_lst,
                "Last_Update": updated_to
            }
        }
    )

    logging.debug(f"spark total time: {time.time() - st} seconds")
