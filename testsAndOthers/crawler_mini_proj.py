import os
from io import TextIOWrapper

from pyspark import SparkContext

from Spark.Spark_handler_class import Spark_handler
from Hadoop.hdfs import HDFS_handler


def get_file() -> TextIOWrapper:
    path: str = r"C:\Users\adam l\Desktop\python files\BigData\Web\scrapy_web_crawler.py"

    os.system(f'scrapy runspider "{path}" -O quotes.jl')  # O for overriding, o for appending to file

    with open("quotes.jl") as file: return file


def save_file_to_hdfs(file_path: str):
    HDFS_handler.start()
    os.system(f"hdfs dfs -put \"{file_path}\" /user/hduser")


def pass_to_spark(file_path: str):
    sc: SparkContext = Spark_handler.spark_context_setup()
    


def main():
    file: TextIOWrapper = get_file()
    file_path: str = f"{os.getcwd()}\\{file.name}"
    # save_file_to_hdfs(file_path=file_path)
    pass_to_spark(file_path=file_path)


if __name__ == '__main__':
    main()
