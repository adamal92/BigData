from sqlite3.dbapi2 import Cursor
from typing import Any, Union
from pyspark import SparkContext, RDD
import sqlite3
import logging

from SQL.SQLite import SQLite_DB

# : read SQLite file and parse it to a matrix
# : write matrix to SQLite
# : parse the matrix to spark
# TODO: save matrix to hdfs
# TODO: read matrix from hdfs

# global
DB_PATH = r'C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db'  # path to the database file

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.WARNING)


def spark_setup(app_name: str, master: str = "local") -> SparkContext:
    sc = SparkContext(master, app_name)
    sc.setLogLevel("ERROR")
    print("---------------------------------------------------------------------------------\n\r")
    return sc


def matrix_spark(sc: SparkContext) -> list:
    """
    parse the matrix to spark
    :param sc :type SparkContext:
    :return:
    """
    table: list = SQLite_DB.get_table("branch", None)
    rdd: Union[RDD, Any] = sc.parallelize(table)
    rdd_elements: list = rdd.collect()
    print(f"Elements in RDD -> {rdd_elements}")
    for row in rdd_elements:
        print(row)
    sc.stop()
    return rdd_elements


def hdfs():
    pass
    # write file to hdfs
    # Create data
    data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
    df = sparkSession.createDataFrame(data)
    # Write into HDFS
    df.write.csv("hdfs://cluster/user/hdfs/test/example.csv")

    # Read file from HDFS
    df_load = sparkSession.read.csv('hdfs://cluster/user/hdfs/test/example.csv')
    df_load.show()

# : move
def sqlite_insert_table(matrix: list, *args, **kwargs) -> int:
    """
    write matrix to SQLite db
    :param matrix :type list: the matrix that holds the data of the inserted table
    :param tablename :type str: name of the newly created table, defaults to 'default_table'
    :param args :type list
    :param kwargs:
    :return :type int: 0 if all is good
    """
    tablename: str = kwargs.get("tablename", None)
    if tablename is None: tablename = "default_table"  # default value
    try:
        conn = sqlite3.connect(DB_PATH)  # open db
        # create table
        try:
            query = "CREATE TABLE %s (branch_id INT PRIMARY KEY, branch_name VARCHAR(40), " \
                    "mgr_id INT, mgr_start_date DATE, FOREIGN KEY(mgr_id) REFERENCES employee(emp_id) " \
                    "ON DELETE SET NULL);" % tablename
            cursor = conn.execute(query)
        except sqlite3.OperationalError:
            logging.error("Couldn't create table %s. The table already exists" % tablename)
        except Exception:
            logging.error("Failed to create table " + tablename)
            raise
        # insert matrix into table
        for row in matrix:
            if None in row:
                continue
            query = f"INSERT INTO {tablename} VALUES({row[0]}, '{row[1]}', {row[2]}, '{row[3]}');"
            logger.warning(query)
            cursor = conn.execute(query)  # INSERT INTO table VALUES(row)
        # end & close the db connection
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logging.log(logging.ERROR, "failed to connect to SQLite DB: "
                                   "sqlite Error\n\rPlease check your SQL command syntax")
        raise e
    except Exception as e:
        logging.log(logging.ERROR, "failed to connect to SQLite DB")
        raise
    return 0  # if succeeded


def exec_sql(query_command: str, db_path: str) -> list:
    """
    Execute a given SQL query command at a given SQLite database
    :param query_command :type str: SQL to be executed
    :param db_path :type str: Location of the (.db) database file
    :return :type list: Contains the result sent back by the database. If None, it means that an error has occurred
    """
    try:
        conn: sqlite3.Connection = sqlite3.connect(db_path)  # open db
        cursor: Cursor = conn.execute(query_command)
        conn.commit()
        ret: list = cursor.fetchall()
        conn.close()
        return ret
    except Exception as e:
        logging.error(e)
        return None


def main():
    # hdfs()
    matrix = matrix_spark(spark_setup("My App"))
    print(exec_sql("DROP TABLE default_table;", DB_PATH))
    sqlite_insert_table(matrix)


if __name__ == '__main__':
    main()
