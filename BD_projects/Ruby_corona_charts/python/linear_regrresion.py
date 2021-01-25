import sys
from datetime import datetime, timedelta
import json
import logging
import os
import time
import winsound
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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import explode, create_map


class Constants:
    db = {}
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'
    SCHEDULER: BlockingScheduler = BlockingScheduler()
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
    to_spark_direct_upside_down(cities=response["result"]["records"])
    
    # citiesDF: DataFrame = spark.createDataFrame(data=cities)
    # process_data(data_frame=citiesDF)


def to_spark_direct_upside_down(cities: dict):
    st = time.time()

    citiesDF: DataFrame = spark.createDataFrame(data=cities)
    citiesDF.cache()

    citiesDF = citiesDF.select(citiesDF.Date, citiesDF.Cumulative_verified_cases, citiesDF.City_Name)
    citiesDF = citiesDF.filter(citiesDF.City_Name == "תל אביב - יפו") \
        .filter(citiesDF.Cumulative_verified_cases != "<15")

    Last_Update: str = citiesDF.toPandas().to_dict()["Date"].pop(citiesDF.count()-1)
    City: str = citiesDF.toPandas().to_dict()["City_Name"].pop(0)

    citiesDF = citiesDF.drop("City_Name")

    citiesDF.show()

    logging.debug(f"spark total time: {time.time() - st} seconds")

    return


# https://towardsdatascience.com/apache-spark-mllib-tutorial-ec6f1cb336a9
def ml_example():
    # load data
    data: DataFrame = spark.read.csv(
        'C:/Users/adam l/Desktop/python files/BigData/BD_projects/Ruby_corona_charts/excel_sheets/boston_housing.csv',
        header=True, inferSchema=True)
    # create features vector
    feature_columns: list = data.columns[:-1]  # here we omit the final column
    from pyspark.ml.feature import VectorAssembler
    assembler: VectorAssembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_2: DataFrame = assembler.transform(data)
    # train/test split
    test: DataFrame
    train: DataFrame
    train, test = data_2.randomSplit([0.7, 0.3])
    # define the model
    from pyspark.ml.regression import LinearRegression
    algo: LinearRegression = LinearRegression(featuresCol="features", labelCol="medv")
    # train the model
    model: pyspark.ml.regression.LinearRegressionModel = algo.fit(train)
    # evaluation
    evaluation_summary: pyspark.ml.regression.LinearRegressionSummary = model.evaluate(test)
    print(
        evaluation_summary.meanAbsoluteError,
        evaluation_summary.rootMeanSquaredError,
        evaluation_summary.r2
    )
    # predicting values
    predictions: DataFrame = model.transform(test)
    predictions.select(
        predictions.columns[13:]
    ).show()  # here I am filtering out some columns just for the figure to fit


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

    ml_example()

    # try:
    #     firebase_config()
    #
    #     if Constants.RUN_SCHEDULER:
    #         Constants.SCHEDULER.start()
    #     else:
    #         crawl_corona()
    # finally:
    #     winsound.MessageBeep(winsound.MB_ICONHAND)
    #     logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
