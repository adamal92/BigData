import logging
import os
import time
from io import TextIOWrapper
from typing import Any, Union, List, KeysView

import py4j
import pyspark
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, RDD
from pyspark.python.pyspark.shell import spark
from pyspark.rdd import PipelinedRDD
from pyspark.sql import DataFrame

from Spark.Spark_handler_class import Spark_handler
from Hadoop.hdfs import HDFS_handler


def get_file() -> TextIOWrapper:
    path: str = r"C:\Users\adam l\Desktop\python files\BigData\Web\scrapy_web_crawler.py"

    os.system(f'scrapy runspider "{path}" -O quotes.jl -L INFO')  # O for overriding, o for appending to file

    with open("quotes.jl") as file: return file


def save_file_to_hdfs(file_path: str):
    os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    os.system("hdfs dfs -rm -R -skipTrash /user/hduser/quotes.jl")  # delete file
    os.system(HDFS_handler.LIST_FILES)
    os.system(f"hdfs dfs -put \"{file_path}\" /user/hduser")  # create file
    os.system(HDFS_handler.LIST_ALL)
    os.system(HDFS_handler.LIST_FILES)
    os.system("hdfs dfsadmin -safemode enter")  # safe mode on


# TODO:
def process_data(data_frame: DataFrame) -> dict:
    sc: SparkContext = Spark_handler.spark_context_setup(log_level="WARN")

    pickle: list = data_frame.collect()  # Union[List[Any], Any] = list
    logging.warning(type(pickle))
    quotes: pyspark.rdd.RDD = sc.parallelize(pickle)  # Union[RDD, Any] = RDD
    logging.warning(type(quotes))

    temp_dict = {}
    temp_list = []
    #     temp_dict: Accumulator = sc.accumulator(dict())
    quotes_list = quotes.collect()

    # create values list (clean data)
    for row in quotes_list:
        dict_str: str = pyspark.sql.types.Row.asDict(row, True)["value"].split(",")[0][1:]  # .split("{")[1]
        # print(dict_str)
        dict_list = dict_str.split(":")
        # print(dict_list[0], dict_list[1][1:])
        temp_dict[dict_list[0].split("\"")[1]] = dict_list[1][1:].split("\"")[1]  # .replace(" ", "_")
        # temp_dict[dict_list[1][1:]] = temp_dict[dict_list[0]]
        temp_list.append(dict_list[1][1:].split("\"")[1].split(" ")[0])  # first names
        # temp_list.append(dict_list[1][1:].split("\"")[1])  # author names

    # create key-value pairs

    # def clean_data(row):
    #     dict_str: str = pyspark.sql.types.Row.asDict(row, True)["value"].split(",")[0][1:]  # .split("{")[1]
    #     # print(dict_str)
    #     dict_list = dict_str.split(":")
    #     print(dict_list[0], dict_list[1][1:])
    #     # temp_dict[dict_list[0]] = dict_list[1][1:]
    #     temp_list.append(dict_list[1][1:].split("\"")[1])
    #
    # quotes.foreach(clean_data)
    # print(temp_dict)

    sc.emptyRDD()
    # print(temp_list)
    author_names = sc.parallelize(temp_list)

    # filter (lambda list.pop : bool)
    # os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/ipython"

    # os.environ["PYSPARK_PYTHON"] = "C:\Spark\spark-3.0.1-bin-hadoop3.2\python"
    # os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\Spark\spark-3.0.1-bin-hadoop3.2\python"

    filtered: PipelinedRDD = author_names.filter(lambda name: name)  # all names
    # filtered: PipelinedRDD = author_names.filter(lambda name: "S" in name)  # create dictionary
    print("Fitered RDD -> %s" % filtered.collect())
    # print("Fitered RDD -> %s" % author_names.filter(lambda name: "S" in name).collect())

    # map (lambda list.pop : key_value_tuple)
    # map (lambda old_element : new_element)
    mapped: PipelinedRDD = filtered.map(lambda x: (x, 0))
    print("Key value pair -> %s" % mapped.collect())
    # quotes = quotes.map(lambda x: print(x))
    logging.critical(dict(mapped.collect()))

    # unique_vals: list = []
    # # for tuple_item in mapped.collect():
    # #     if not tuple_item in unique_vals:
    # #         unique_vals.append(tuple_item)
    # #     else:
    # #         for item in unique_vals:
    # #             if item[0] == tuple_item[0]:
    # #                 item[1] = int(item[1]) + int(tuple_item[1])
    # dictionary_vals: dict = dict(mapped.collect())
    # print(dictionary_vals)
    # for key in dictionary_vals:
    #     if not key in unique_vals:
    #         # print(key, type(key), dictionary_vals[key], type(dictionary_vals[key]))
    #         unique_vals.append((key, dictionary_vals[key]))
    #     else:
    #         for item in unique_vals:
    #             if item[0] == key:
    #                 dictionary_vals[key] = int(dictionary_vals[key]) + int(item[1])
    # logging.critical(unique_vals)

    # count number of items (reduce)
    dictionary_vals: dict = dict(mapped.collect())  # without duplicates
    for item in mapped.collect():
        dictionary_vals[item[0]] += 1


    logging.critical(dictionary_vals)
    sc.emptyRDD()

    return dictionary_vals

    # mapped = sc.parallelize(unique_vals)
    # print(mapped.collect())
    #
    # # reduce (lambda key, values_list : reduced_tuple)
    # from operator import add
    # # adding: tuple = quotes.reduce(add)
    # # print(adding, type(adding))
    # adding: tuple = mapped.reduce(f=lambda key, values_list: print(key, values_list))
    # print(adding)
    # print(mapped.collect())
    # # print(type(quotes.filter(lambda : None)), type(quotes.map(lambda : None)), type(quotes.reduce(lambda a,b: None)))


def pass_to_spark(file_path: str) -> dict:
    try:
        sc: SparkContext = Spark_handler.spark_context_setup(log_level="WARN")
        df: DataFrame = spark.read.text(f"{HDFS_handler.DEFAULT_CLUSTER_PATH}user/hduser/quotes.jl")  # core-site.xml
        # df.show()

        count_names: dict = process_data(data_frame=df)
        sc.stop()
        return count_names

    except Py4JJavaError as e:
        logging.debug(type(e.java_exception))
        if "java.net.ConnectException" in e.java_exception.__str__():
            logging.error("HDFS cluster is down")
        else:
            HDFS_handler.stop()
            raise
    except:
        HDFS_handler.stop()
        raise


# TODO: web crawler (scrapy) -> cluster (HDFS) ->
# TODO: map-reduce (spark) -> NoSQL (elasticsearch) -> SQL (SQLite) -> visualization (matplotlib)
def main():
    file: TextIOWrapper = get_file()
    file_path: str = f"{os.getcwd()}\\{file.name}"
    # file_path: str = f"{os.getcwd()}\\quotes.jl"
    HDFS_handler.start()
    save_file_to_hdfs(file_path=file_path)
    # os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    json_count_names: dict = pass_to_spark(file_path=file_path)
    HDFS_handler.stop()



if __name__ == '__main__':
    main()
