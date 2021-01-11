import os
# os.chdir(")
# os.curdir()

# if 'SPARK_HOME' not in os.environ:
#     os.environ['SPARK_HOME'] = ""

# SPARK_HOME = os.environ['SPARK_HOME']

# import sys
# sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
from pyspark import SparkConf, SparkContext

conf = SparkConf()
# conf.set("spark.executor.memory", "1g")
# conf.set("spark.cors.max", "2")
conf.setAppName("name")

# for streaming' create a spark context with 2 threads
sc = SparkContext('local[2]', conf=conf)