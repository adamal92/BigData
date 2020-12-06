import logging
import os

import time
from io import TextIOWrapper
import subprocess, sys, time
from typing import Any, Union, List, KeysView

import matplotlib
import matplotlib.pyplot
import pandas

import pyspark
import requests
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, RDD
from pyspark.python.pyspark.shell import spark
from pyspark.rdd import PipelinedRDD
from pyspark.sql import DataFrame
from requests import Response

from NoSQL.ElasticSearch.elasticsearch_handler import Elasticsearch_Handler
from SQL.SQLite_database_handler import SQLite_handler
from Spark.Spark_handler_class import Spark_handler
from Hadoop.hdfs import HDFS_handler
from testsAndOthers.data_types_and_structures import DataTypesHandler


def get_file() -> TextIOWrapper:
    # path: str = r"C:\Users\adam l\Desktop\python files\BigData\Web\scrapy_web_crawler.py"
    # print(os.getcwd(), __file__, os.name)

    scrapy_crawler_path: str = ""
    for directory in os.path.dirname(__file__).split("/")[:-1]:
        scrapy_crawler_path += f"{directory}\\"
    scrapy_crawler_path += r"Web\scrapy_web_crawler.py"

    # path: str = os.getcwd() + r"\scrapy_web_crawler.py"

    os.system(f'scrapy runspider "{scrapy_crawler_path}" -O quotes.jl -L ERROR')  # O for overriding, o for appending to file

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
    sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

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
        sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

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


def upload_json_to_elastic(json: dict):
    # print(os.path.dirname(__file__).split("/")[:-1])
    elastic_path: str = ""
    for directory in os.path.dirname(__file__).split("/")[:-1]:
        elastic_path += f"{directory}\\"
    elastic_path += "NoSQL\\ElasticSearch"
    p = subprocess.Popen(["python", f'{elastic_path}\\start_search.py'], stdout=sys.stdout)  # search
    # p2 = subprocess.Popen(["python", f'{elastic_path}\\start_kibana.py'], stdout=sys.stdout)  # kibana
    # p.communicate()  # wait for process to end

    time.sleep(15)  # minimum time that elasticsearch takes to start: 13

    Elasticsearch_Handler.exec(fn=lambda url: requests.put(url=url + f"school/_doc/quotes", json=json),
                               print_recursively=True,
                               print_form=DataTypesHandler.PRINT_DICT)


def from_elastic_to_sqlite():
    # get table from elastic
    response: Response = Elasticsearch_Handler.exec(fn=lambda url: requests.get(url + "school/_doc/quotes"),
                                                    print_recursively=True, print_form=DataTypesHandler.PRINT_DICT)
    logging.warning(response.json()["_source"])
    json_dict: dict = response.json()["_source"]
    # print(json_dict, type(json_dict))
    names_list: list = DataTypesHandler.dict_to_matrix(dictionary=json_dict)
    logging.warning(names_list)

    # insert table into sqlite
    sqlithndlr: SQLite_handler = \
        SQLite_handler(db_path=r"C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db")
    print(sqlithndlr.db_path)
    SQLite_handler.exec_all(sqlithndlr.db_path, SQLite_handler.GET_TABLES, "DROP TABLE crawler_names;")
    schema: str = "first_author_name VARCHAR(40), quotes_count INT"
    print(SQLite_handler.sqlite_insert_table(tablename="crawler_names", table_schema=schema, matrix=names_list,
                                             db_path=sqlithndlr.db_path))


# TODO: response = Elasticsearch_Handler.exec(filename)
# TODO: table = DataTypesHandler.json_to_table(table)
# TODO: sqlitHandler.addTable(table)


def visualize_sqlite():

    # table: list = SQLite_handler.get_table(tablename="crawler_names", db_path=SQLite_handler.db_path)
    # DataTypesHandler.print_data_recursively(data=table, print_dict=DataTypesHandler.PRINT_ARROWS)
    # VisualizationHandler.visualize_matrix(table)

    # get data from elastic
    response: Response = Elasticsearch_Handler.exec(fn=lambda url: requests.get(url + "school/_doc/quotes"),
                                                    print_recursively=True, print_form=DataTypesHandler.PRINT_DICT)
    logging.warning(response.json()["_source"])
    pass_dict: dict = response.json()["_source"]

    # visualize data
    # dataframe: pandas.DataFrame = pandas.DataFrame(data=pass_dict, index=[0])
    dataframe = pandas.DataFrame.from_records( [pass_dict] )
    df_lists = dataframe[list(pass_dict.keys())].unstack().apply(pandas.Series)
    df_lists.plot.bar(rot=0, cmap=matplotlib.pyplot.cm.jet, fontsize=8, width=0.7, figsize=(8, 4))
    matplotlib.pyplot.show()


# TODO: visualize_handler.visualizeTable(sqlite_handler.getTable())


# TODO: web crawler (scrapy) -> cluster (HDFS) ->
# TODO: map-reduce (spark) -> NoSQL (elasticsearch) -> SQL (SQLite) -> visualization (matplotlib)
def main():
    start = time.time()

    # loggers
    py4j_logger = logging.getLogger('py4j.java_gateway')  # py4j logs
    py4j_logger.setLevel(logging.ERROR)

    matplotlib_logger = logging.getLogger('matplotlib')  # matplotlib logs
    matplotlib_logger.setLevel(logging.ERROR)

    logging.basicConfig(level=logging.WARNING)

    # scrapy
    file: TextIOWrapper = get_file()
    file_path: str = f"{os.getcwd()}\\{file.name}"
    # file_path: str = f"{os.getcwd()}\\quotes.jl"

    # hdfs & spark
    HDFS_handler.start()
    save_file_to_hdfs(file_path=file_path)
    time.sleep(2)
    # os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    json_count_names: dict = pass_to_spark(file_path=file_path)
    HDFS_handler.stop()

    # elastic
    upload_json_to_elastic(json=json_count_names)

    from_elastic_to_sqlite()

    # matplotlib
    visualize_sqlite()

    print("OK Total Time: %s" % (time.time() - start))


if __name__ == '__main__':
    main()
