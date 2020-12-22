# WARNING: before running this script make sure that Spark core is installed on your machine

# imports
import logging

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, RDD
from typing import Any, Union

from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame


# global
from Hadoop.hdfs import HDFS_handler


class Spark_handler:
    """

    """

    def __init__(self, appname: str = "MyApp", master: str = "local"):
        Spark_handler.spark_context_setup(app_name=appname, master=master)

    def __str__(self):
        print("spark handler")

    @staticmethod
    def spark_context_setup(app_name: str = "MyApp", master: str = "local", log_level: str = "ERROR") -> SparkContext:
        """
        Create a new spark context & return it
        :param app_name :type str:
        :param master :type str:
        :param log_level :type str:
        :return :type pyspark.SparkContext:
        """
        try:
            sc = SparkContext(master, app_name)
        except ValueError:
            sc: SparkContext = spark.sparkContext
        sc.setLogLevel(log_level)
        # logger = sc._jvm.org.apache.log4j
        # logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
        # logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)
        return sc

    @staticmethod
    def spark_session_setup(app_name: str = "MyApp", master: str = "local", log_level: str = "ERROR") -> SparkSession:
        """
        Create a new spark session & return it
        :param app_name :type str:
        :param master :type str:
        :param log_level :type str:
        :return :type pyspark.sql.SparkSession:
        """
        sparkSession: SparkSession = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        print("---------------------------------------------------------------------------------\n\r")

        # SparkSession.stop(sparkSession)
        # sparkSession.stop()
        return sparkSession

    # : clean
    @staticmethod
    def matrix_spark(sc: SparkContext, table: list) -> list:
        """
        parse the matrix to spark
        :param sc :type SparkContext:
        :param table :type list:
        :return :type list: all elements that are in the RDD
        """
        rdd: Union[RDD, Any] = sc.parallelize(table)
        rdd_elements: list = rdd.collect()
        print(f"Elements in RDD -> {rdd_elements}")
        for row in rdd_elements:
            print(row)
        sc.stop()
        return rdd_elements

    @staticmethod
    def pass_to_spark(process_fn, file_path: str) -> dict:
        """
        Passes a given file to spark and processes it with a given function
        :param process_fn :type function(data_frame: DataFrame) -> dict: the function that processes the given file
        :param file_path :type str: the path of the file that should be processed
        :return :type dict:
        """
        try:
            sc: SparkContext = Spark_handler.spark_context_setup(log_level="ERROR")

            df: DataFrame = spark.read.text(file_path)  # core-site.xml
            # df.show()

            count_names: dict = process_fn(data_frame=df)
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
