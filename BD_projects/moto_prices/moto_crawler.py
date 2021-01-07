# imports
import logging
import os

from io import TextIOWrapper
import subprocess, sys, time
from typing import Dict, List, Any

from pyspark.sql import DataFrame

from Hadoop.hdfs import HDFS_handler

DIRS_TILL_ROOT: int = 3
DELIMETER = "\\"  # "/"


class MotoCrawler:
    motorcycle_file: TextIOWrapper = None

    @staticmethod
    def get_file(save_as: str="motorcycles.json") -> TextIOWrapper:
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

        with open(save_as) as file:
            MotoCrawler.motorcycle_file = file
            return file

    @staticmethod
    def start_scrapy_spider(save_as: str = "motorcycles.json",
                            spider_py: str = r"BigData\BD_projects\moto_prices\scrapy_spider.py",
                            dirs_till_root: int = 3, delimeter: bool = True) -> TextIOWrapper:
        """

        :return:
        """
        if delimeter: delim = "\\"
        else: delim = "/"

        scrapy_crawler_path: str = ""
        for directory in os.path.dirname(__file__).split(delim)[:-dirs_till_root]:
            scrapy_crawler_path += f"{directory}\\"
        cwd_path = scrapy_crawler_path
        scrapy_crawler_path += spider_py

        # O for overriding, o for appending to file
        os.system(f'scrapy runspider "{scrapy_crawler_path}" -O {save_as} -L ERROR')

        with open(save_as) as file:
            MotoCrawler.motorcycle_file = save_as
            return file

    @staticmethod
    def save_file_to_hdfs(file_path: str, filename: str):
        """

        :param file_path:
        :return:
        """
        os.system("hdfs dfsadmin -safemode leave")  # safe mode off
        time.sleep(2)
        os.system(f"hdfs dfs -rm -R -skipTrash /user/hduser/{filename}")  # delete file
        os.system(HDFS_handler.LIST_FILES)
        os.system(f"hdfs dfs -put \"{file_path}\" /user/hduser")  # create file
        os.system(HDFS_handler.LIST_ALL)
        os.system(HDFS_handler.LIST_FILES)
        os.system("hdfs dfsadmin -safemode enter")  # safe mode on

    @staticmethod
    def process_data(data_frame: DataFrame) -> Dict:
        # TODO: add new miners/processing-tasks

        return MotoCrawler.count_moto_years(data_frame=data_frame)

    @staticmethod
    def count_moto_years(data_frame: DataFrame) -> Dict:
        from pyspark import SparkContext
        import pyspark
        from Spark.Spark_handler_class import Spark_handler
        sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

        pickle: list = data_frame.collect()  # Union[List[Any], Any] = list
        logging.warning(type(pickle))
        quotes: pyspark.rdd.RDD = sc.parallelize(pickle)  # Union[RDD, Any] = RDD
        logging.warning(type(quotes))

        temp_dict = {}
        temp_list = []
        #     temp_dict: Accumulator = sc.accumulator(dict())
        moto_bikes_list: List[pyspark.Row] = quotes.collect()
        # print(moto_bikes_list)
        # from testsAndOthers.data_types_and_structures import DataTypesHandler
        # DataTypesHandler.print_data_recursively(moto_bikes_list)

        # create values list (clean data)
        for row in moto_bikes_list:
            if row.value == "[" or row.value == "]": continue

            dict_str: str = pyspark.sql.types.Row.asDict(row, True)["value"].split(",")[0][1:]  # .split("{")[1]
            # print(dict_str)
            dict_list = dict_str.split(":")
            # print(dict_list[0], dict_list[1][1:])
            temp_dict[dict_list[0].split("\"")[1]] = dict_list[1][1:].split("\"")[1]  # .replace(" ", "_")
            # temp_dict[dict_list[1][1:]] = temp_dict[dict_list[0]]
            temp_list.append(dict_list[1][1:].split("\"")[1].split(" ")[0])  # first names
            # temp_list.append(dict_list[1][1:].split("\"")[1])  # author names

        # create key-value pairs
        sc.emptyRDD()
        author_names = sc.parallelize(temp_list)

        # filter (lambda list.pop : bool)
        from pyspark.rdd import PipelinedRDD
        filtered: PipelinedRDD = author_names.filter(lambda name: name)  # all names
        print("Fitered RDD -> %s" % filtered.collect())

        # map (lambda list.pop : key_value_tuple)
        # map (lambda old_element : new_element)
        mapped: PipelinedRDD = filtered.map(lambda x: (x, 0))
        print("Key value pair -> %s" % mapped.collect())
        logging.critical(dict(mapped.collect()))

        # count number of items (reduce)
        dictionary_vals: dict = dict(mapped.collect())  # without duplicates
        for item in mapped.collect():
            dictionary_vals[item[0]] += 1

        logging.critical(dictionary_vals)
        sc.emptyRDD()

        return dictionary_vals

    @staticmethod
    def count_moto_models(data_frame: DataFrame) -> Dict:
        from pyspark import SparkContext
        import pyspark
        from Spark.Spark_handler_class import Spark_handler
        sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

        pickle: list = data_frame.collect()  # Union[List[Any], Any] = list
        logging.warning(type(pickle))
        quotes: pyspark.rdd.RDD = sc.parallelize(pickle)  # Union[RDD, Any] = RDD
        logging.warning(type(quotes))

        temp_dict = {}
        temp_list = []
        #     temp_dict: Accumulator = sc.accumulator(dict())
        moto_bikes_list: List[Any] = quotes.collect()

        # create values list (clean data)
        for row in moto_bikes_list:
            dict_str: str = pyspark.sql.types.Row.asDict(row, True)["value"].split(",")[0][1:]  # .split("{")[1]
            # print(dict_str)
            dict_list = dict_str.split(":")
            # print(dict_list[0], dict_list[1][1:])
            temp_dict[dict_list[0].split("\"")[1]] = dict_list[1][1:].split("\"")[1]  # .replace(" ", "_")
            # temp_dict[dict_list[1][1:]] = temp_dict[dict_list[0]]
            temp_list.append(dict_list[1][1:].split("\"")[1].split(" ")[0])  # first names
            # temp_list.append(dict_list[1][1:].split("\"")[1])  # author names

        # create key-value pairs
        sc.emptyRDD()
        author_names = sc.parallelize(temp_list)

        # filter (lambda list.pop : bool)
        from pyspark.rdd import PipelinedRDD
        filtered: PipelinedRDD = author_names.filter(lambda name: name)  # all names
        print("Fitered RDD -> %s" % filtered.collect())

        # map (lambda list.pop : key_value_tuple)
        # map (lambda old_element : new_element)
        mapped: PipelinedRDD = filtered.map(lambda x: (x, 0))
        print("Key value pair -> %s" % mapped.collect())
        logging.critical(dict(mapped.collect()))

        # count number of items (reduce)
        dictionary_vals: dict = dict(mapped.collect())  # without duplicates
        for item in mapped.collect():
            dictionary_vals[item[0]] += 1

        logging.critical(dictionary_vals)
        sc.emptyRDD()

        return dictionary_vals

    @staticmethod
    def upload_json_to_elastic(json: dict):
        pass
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

# def main():
#     start = time.time()
#
#     # loggers
#     py4j_logger = logging.getLogger('py4j.java_gateway')  # py4j logs
#     py4j_logger.setLevel(logging.ERROR)
#
#     matplotlib_logger = logging.getLogger('matplotlib')  # matplotlib logs
#     matplotlib_logger.setLevel(logging.ERROR)
#
#     # logging.basicConfig(level=logging.WARNING)
#
#     # scrapy
#     file: TextIOWrapper = get_file()
#     file_path: str = f"{os.getcwd()}\\{file.name}"
#     # file_path: str = f"{os.getcwd()}\\quotes.jl"
#     logging.debug(file_path)
#
#     print("OK Total Time: %s seconds" % (time.time() - start))

