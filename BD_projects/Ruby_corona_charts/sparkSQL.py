# TODO: combine with load_json, add crawler (input) & firebase (output) & scheduler (batch timing)
import logging
import time
import winsound
from typing import Any, Union

from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame, Column

from Spark.Spark_handler_class import Spark_handler


def to_spark():
    st = time.time()



    # json_count_names: dict = Spark_handler.pass_to_spark(
    #     file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}{HDFS_handler.HADOOP_USER}/{CITY_JSON}",
    #     process_fn=process_data
    # )

    spark_session = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()

    # print(spark_session.read.json("json/city_json.json"))
    df: DataFrame = spark_session.read.json("json/city_json.json")
    # df.printSchema()
    # df.show()
    # cities: Column = df.result.records
    # cities: Column = df["result"]["records"]
    #
    # # df.select(df.result.records.getField("City_Name")).show()
    # df.select(cities.getField("City_Name")).show()
    # df.select(cities("City_Name")).show()
    df.createOrReplaceTempView("table_1")

    df2: DataFrame = spark.sql("SELECT result from table_1")
    # print(df2.collect())
    df2.printSchema()
    df2.createOrReplaceTempView("table_2")

    df3: DataFrame = spark.sql("SELECT result from table_2")
    df3.printSchema()
    df3.show()
    print(df3.collect())
    print(df3.select("records"))
    df3.createOrReplaceTempView("table_3")

    cities_records: DataFrame = spark.sql("SELECT records from table_3")
    cities_records.printSchema()


    # from testsAndOthers.data_types_and_structures import DataTypesHandler, PrintForm
    # DataTypesHandler.print_data_recursively(data=json_count_names, print_dict=PrintForm.PRINT_DICT.value)

    logging.debug(f"spark total time: {time.time() - st} seconds")


def main():
    st = time.time()

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('flaskwebgui').setLevel(logging.ERROR)
    logging.getLogger('BaseHTTPRequestHandler').setLevel(logging.ERROR)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('my_log').setLevel(logging.DEBUG)

    try:
        to_spark()
    finally:
        winsound.MessageBeep(winsound.MB_ICONHAND)
        logging.debug(f"Program Total Time: {time.time() - st} seconds")


if __name__ == '__main__':
    main()
