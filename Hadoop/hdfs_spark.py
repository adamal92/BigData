from pyspark.sql import SparkSession


def hdfs():
    sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

    # write file to hdfs
    # Create data
    data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
    df = sparkSession.createDataFrame(data)
    # Write into HDFS
    # df.write.csv("user/hdfs/test/example.csv")  # hdfs://cluster/user/hdfs/test/example.csv

    # Read file from HDFS
    df_load = sparkSession.read.csv('user/hdfs/test/example.csv')  # hdfs://cluster/user/hdfs/test/example.csv
    df_load.show()


hdfs()

from Hadoop.hdfs import HDFS_handler
import os
# TODO: move to tests
HDFS_handler.start()
os.system(HDFS_handler.HELP)
os.system(HDFS_handler.LIST_ALL)
os.system(HDFS_handler.LIST_FILES)
os.system(r"hdfs dfs -ls /user/test")
os.system(r"hdfs dfs -ls /user")