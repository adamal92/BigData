# Code taken from: https://stackoverflow.com/questions/37513355/converting-pandas-dataframe-into-spark-dataframe-error
import pandas

import pyspark
from pyspark.python.pyspark.shell import spark
from pyspark.sql.types import *


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()


def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df: pandas.DataFrame) -> pyspark.sql.DataFrame:
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)
