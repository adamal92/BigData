import logging
import os
from io import TextIOWrapper

import py4j
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import DataFrame

from Spark.Spark_handler_class import Spark_handler
from Hadoop.hdfs import HDFS_handler


def get_file() -> TextIOWrapper:
    path: str = r"C:\Users\adam l\Desktop\python files\BigData\Web\scrapy_web_crawler.py"

    os.system(f'scrapy runspider "{path}" -O quotes.jl')  # O for overriding, o for appending to file

    with open("quotes.jl") as file: return file


def save_file_to_hdfs(file_path: str):
    os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    os.system("hdfs dfs -rm -R -skipTrash /user/hduser/quotes.jl")  # delete file
    os.system(HDFS_handler.LIST_FILES)
    os.system(f"hdfs dfs -put \"{file_path}\" /user/hduser")  # create file
    os.system(HDFS_handler.LIST_ALL)
    os.system(HDFS_handler.LIST_FILES)
    os.system("hdfs dfsadmin -safemode enter")  # safe mode on


def pass_to_spark(file_path: str):
    sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

    # df_load = sparkSession.read.csv('user/hdfs/test/example.csv')  # hdfs://cluster/user/hdfs/test/example.csv
    try:
        df: DataFrame = spark.read.text("hdfs://localhost:9820/user/hduser/quotes.jl")  # core-site.xml
        df.show()
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


def main():
    file: TextIOWrapper = get_file()
    file_path: str = f"{os.getcwd()}\\{file.name}"
    HDFS_handler.start()
    save_file_to_hdfs(file_path=file_path)
    pass_to_spark(file_path=file_path)
    HDFS_handler.stop()


if __name__ == '__main__':
    main()
