# WARNING: before running this script make sure that Spark core is installed on your machine

# imports
from pyspark import SparkContext, RDD
from typing import Any, Union

# global


class Spark_handler:
    """

    """

    def __init__(self, appname: str = "MyApp", master: str = "local"):
        Spark_handler.spark_setup(app_name=appname, master=master)

    def __str__(self):
        print("spark handler")

    @staticmethod
    def spark_setup(app_name: str="MyApp", master: str = "local", log_level: str = "ERROR") -> SparkContext:
        """
        Create a new spark context & return it
        :param app_name :type str:
        :param master :type str:
        :param log_level :type str:
        :return :type pyspark.SparkContext:
        """
        sc = SparkContext(master, app_name)
        sc.setLogLevel(log_level)
        print("---------------------------------------------------------------------------------\n\r")
        return sc

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
