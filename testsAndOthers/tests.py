from pyspark.sql import SparkSession

sparkSession: SparkSession = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
print("---------------------------------------------------------------------------------\n\r")

df = sparkSession.read.text("file:///C:/Users/adam l/Desktop/main.cpp")

df.show()

SparkSession.stop(sparkSession)
# sparkSession.stop()

# --------------------------------------------------------------------
# sqlite

from SQL.SQLite_database_handler import SQLite_handler

# get table & exec all
sqlithndlr: SQLite_handler = \
    SQLite_handler(db_path=r"C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db")
SQLite_handler.exec_all(sqlithndlr.db_path, SQLite_handler.GET_TABLES, "SELECT * FROM sqlite_master;",
                        "DROP TABLE newtable;")
table: list = SQLite_handler.get_table(tablename="employee", db_path=sqlithndlr.db_path)
SQLite_handler.print_2D_matrix(table)

# insert table
schema: str = "emp_id INT PRIMARY KEY, first_name VARCHAR(40), " \
              "last_name VARCHAR(40), birth_day DATE, sex VARCHAR(1), salary INT, super_id INT," \
              " branch_id INT," \
              " CONSTRAINT branch_id FOREIGN KEY(branch_id) REFERENCES branch(branch_id) ON DELETE SET NULL, " \
              "CONSTRAINT super_id FOREIGN KEY(emp_id) REFERENCES employee(emp_id) ON DELETE SET NULL"
print(SQLite_handler.sqlite_insert_table(tablename="newtable", table_schema=schema, matrix=table,
                                         db_path=sqlithndlr.db_path))
table: list = SQLite_handler.get_table(tablename="newtable", db_path=sqlithndlr.db_path)
SQLite_handler.print_2D_matrix(table)

# TODO: test Spark_handler_class
# TODO: test SQLite_handler.sqlite_insert_table

# ---------------------------------------------------------------------
# spark

from Spark.Spark_handler_class import Spark_handler

table: list = SQLite_handler.get_table(tablename="branch", db_path=sqlithndlr.db_path)
print(table)
Spark_handler.matrix_spark(sc=Spark_handler.spark_context_setup(), table=table)

# ----------------------------------------------------------------------
# hdfs

from Hadoop.hdfs import HDFS_handler as hfs
import os
# : move to tests
hfs.start()
os.system(hfs.HELP)
os.system(hfs.LIST_ALL)
os.system(hfs.LIST_FILES)
os.system(r"hdfs dfs -ls /user/test")
os.system(r"hdfs dfs -ls /user")

# ----------------------------------------------------------------------
# visual

from visualization.visualization import VisualizationHandler

VisualizationHandler.visualize_matrix(
    [(1, 2, 3), (3, 4, 5)]
)

SQLite_handler.exec_all(SQLite_handler.db_path, SQLite_handler.GET_TABLES)

VisualizationHandler.visualize_matrix(SQLite_handler
                                      .get_table(tablename="works_with", db_path=SQLite_handler.db_path))

# -----------------------------------------------------------------------
# admin

# from testsAndOthers.administrator_handler import Admin_Handler
import sys
#
# @Admin_Handler.start_as_admin

#
#
# with open("administrator_handler.py", "r") as admin_file:
#     with open(__file__, "a") as exec_file:
#         exec_file.write(admin_file.read())
#         exec_file.write('if __name__ == \'__main__\':\n\tsys.exit(Admin_Handler.start_as_admin(admin_func)')
#     with open(__file__, "r") as exec_file:
#         print(exec_file.read())
#

# if __name__ == '__main__':
    # sys.exit(Admin_Handler.start_as_admin(func=fun))
    # fun(Admin_Handler)
    # fun()
    # sys.exit(Admin_Handler.start_as_admin(lambda : print("ok")))