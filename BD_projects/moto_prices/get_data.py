# imports
import logging
import os

from io import TextIOWrapper
import subprocess, sys, time

import matplotlib
import matplotlib.pyplot
import pandas

# import pyspark
import requests
from py4j.protocol import Py4JJavaError
# from pyspark import SparkContext, RDD
# from pyspark.python.pyspark.shell import spark
# from pyspark.rdd import PipelinedRDD
# from pyspark.sql import DataFrame
from requests import Response

from NoSQL.ElasticSearch.elasticsearch_handler import Elasticsearch_Handler
from SQL.SQLite_database_handler import SQLite_handler
# from Spark.Spark_handler_class import Spark_handler
from Hadoop.hdfs import HDFS_handler
from testsAndOthers.data_types_and_structures import DataTypesHandler

DIRS_TILL_ROOT: int = 3
DELIMETER = "\\"  # "/"


def get_file(save_as: str="motorcycles.jl") -> TextIOWrapper:
    """

    :return:
    """
    scrapy_crawler_path: str = ""
    for directory in os.path.dirname(__file__).split(DELIMETER)[:-DIRS_TILL_ROOT]:
        scrapy_crawler_path += f"{directory}\\"
    cwd_path = scrapy_crawler_path
    scrapy_crawler_path += r"BigData\BD_projects\moto_prices\scrapy_spider.py"

    # O for overriding, o for appending to file
    os.system(f'scrapy runspider "{scrapy_crawler_path}" -O {save_as} -L ERROR')

    with open(save_as) as file: return file


def save_file_to_hdfs(file_path: str):
    """

    :param file_path:
    :return:
    """
    os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    os.system("hdfs dfs -rm -R -skipTrash /user/hduser/quotes.jl")  # delete file
    os.system(HDFS_handler.LIST_FILES)
    os.system(f"hdfs dfs -put \"{file_path}\" /user/hduser")  # create file
    os.system(HDFS_handler.LIST_ALL)
    os.system(HDFS_handler.LIST_FILES)
    os.system("hdfs dfsadmin -safemode enter")  # safe mode on


# def process_data(data_frame: DataFrame) -> dict:
#     """
#
#     :param data_frame:
#     :return:
#     """
#     sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")
#
#     pickle: list = data_frame.collect()  # Union[List[Any], Any] = list
#     logging.warning(type(pickle))
#     quotes: pyspark.rdd.RDD = sc.parallelize(pickle)  # Union[RDD, Any] = RDD
#     logging.warning(type(quotes))
#
#     temp_dict = {}
#     temp_list = []
#     #     temp_dict: Accumulator = sc.accumulator(dict())
#     quotes_list = quotes.collect()
#
#     # create values list (clean data)
#     for row in quotes_list:
#         dict_str: str = pyspark.sql.types.Row.asDict(row, True)["value"].split(",")[0][1:]  # .split("{")[1]
#         # print(dict_str)
#         dict_list = dict_str.split(":")
#         # print(dict_list[0], dict_list[1][1:])
#         temp_dict[dict_list[0].split("\"")[1]] = dict_list[1][1:].split("\"")[1]  # .replace(" ", "_")
#         # temp_dict[dict_list[1][1:]] = temp_dict[dict_list[0]]
#         temp_list.append(dict_list[1][1:].split("\"")[1].split(" ")[0])  # first names
#         # temp_list.append(dict_list[1][1:].split("\"")[1])  # author names
#
#     # create key-value pairs
#     sc.emptyRDD()
#     author_names = sc.parallelize(temp_list)
#
#     # filter (lambda list.pop : bool)
#     filtered: PipelinedRDD = author_names.filter(lambda name: name)  # all names
#     print("Fitered RDD -> %s" % filtered.collect())
#
#     # map (lambda list.pop : key_value_tuple)
#     # map (lambda old_element : new_element)
#     mapped: PipelinedRDD = filtered.map(lambda x: (x, 0))
#     print("Key value pair -> %s" % mapped.collect())
#     logging.critical(dict(mapped.collect()))
#
#     # count number of items (reduce)
#     dictionary_vals: dict = dict(mapped.collect())  # without duplicates
#     for item in mapped.collect():
#         dictionary_vals[item[0]] += 1
#
#     logging.critical(dictionary_vals)
#     sc.emptyRDD()
#
#     return dictionary_vals


def upload_json_to_elastic(json: dict):
    """

    :param json:
    :return:
    """
    elastic_path: str = ""
    for directory in os.path.dirname(__file__).split(DELIMETER)[:-DIRS_TILL_ROOT]:
        elastic_path += f"{directory}\\"
    elastic_path += "NoSQL\\ElasticSearch"
    # TODO: close elastic
    p = subprocess.Popen(["python", f'{elastic_path}\\start_search.py'], stdout=sys.stdout)  # search
    # p2 = subprocess.Popen(["python", f'{elastic_path}\\start_kibana.py'], stdout=sys.stdout)  # kibana
    # p.communicate()  # wait for process to end

    time.sleep(13)  # minimum time that elasticsearch takes to start: 13

    # max_tries = 5
    # counter = 0
    # page = ''
    # # for counter in range(0, max_tries):
    # while page == '':
    #     try:
    #         page = requests.get(Elasticsearch_Handler.DEFAULT_URL)
    #         break
    #     except:
    #         print("Connection refused by the server..")
    #         print("Let me sleep for 5 seconds")
    #         print("ZZzzzz...")
    #         time.sleep(5)
    #         print("Was a nice sleep, now let me continue...")
    #         if counter >= max_tries: break
    #         counter += 1
    #         continue
    #
    # Elasticsearch_Handler.exec(fn=lambda url: requests.put(url=url + f"school/_doc/quotes", json=json),
    #                            print_recursively=True,
    #                            print_form=DataTypesHandler.PRINT_DICT)

    Elasticsearch_Handler.send_request(fn=lambda url: requests.put(url=url + f"school/_doc/quotes", json=json),
                                       print_recursively=True, max_tries=5,
                                       print_form=DataTypesHandler.PRINT_DICT)


def from_elastic_to_sqlite():
    """

    :return:
    """
    # get table from elastic
    response: Response = Elasticsearch_Handler.send_request(fn=lambda url: requests.get(url + "school/_doc/quotes"),
                                                            print_recursively=True,
                                                            print_form=DataTypesHandler.PRINT_DICT, max_tries=5)
    logging.warning(response.json()["_source"])
    json_dict: dict = response.json()["_source"]
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


def visualize_json():
    """

    :return:
    """
    # get data from elastic
    response: Response = Elasticsearch_Handler.send_request(
        fn=lambda url: requests.get(url + "school/_doc/quotes"),
        print_recursively=True, print_form=DataTypesHandler.PRINT_DICT, max_tries=5
    )

    logging.warning(response.json()["_source"])
    pass_dict: dict = response.json()["_source"]

    # visualize data
    from visualization.visualization import VisualizationHandler
    VisualizationHandler.visualize_dictionary(pass_dict)


# TODO: visualize_handler.visualizeTable(sqlite_handler.getTable())


# TODO: web crawler (scrapy) -> cluster (HDFS) ->
# TODO: map-reduce (spark) -> NoSQL (elasticsearch) -> SQL (SQLite) -> visualization (matplotlib)
# TODO: create/add to lib

# TODO: client (js) -> web crawler (scrapy) -> cluster (HDFS) ->
# TODO: map-reduce (spark) -> NoSQL (elasticsearch)/SQL (SQLite) -> client visualization (js)
def main():
    start = time.time()

    # loggers
    py4j_logger = logging.getLogger('py4j.java_gateway')  # py4j logs
    py4j_logger.setLevel(logging.ERROR)

    matplotlib_logger = logging.getLogger('matplotlib')  # matplotlib logs
    matplotlib_logger.setLevel(logging.ERROR)

    # logging.basicConfig(level=logging.WARNING)

    # scrapy
    file: TextIOWrapper = get_file()
    file_path: str = f"{os.getcwd()}\\{file.name}"
    # file_path: str = f"{os.getcwd()}\\quotes.jl"
    logging.debug(file_path)

    # # hdfs & spark
    # HDFS_handler.start()
    # save_file_to_hdfs(file_path=file_path)
    # time.sleep(2)
    # # os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    # json_count_names: dict = Spark_handler.pass_to_spark(
    #     file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}user/hduser/quotes.jl", process_fn=process_data
    # )
    # HDFS_handler.stop()
    #
    # # elastic
    # upload_json_to_elastic(json=json_count_names)
    #
    # from_elastic_to_sqlite()
    #
    # # matplotlib
    # visualize_json()

    print("OK Total Time: %s seconds" % (time.time() - start))


if __name__ == '__main__':
    main()
