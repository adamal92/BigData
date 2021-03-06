# imports
import sqlite3
import logging

# global
from typing import List, Any, Tuple, Dict, Union


class SQLite_handler(object):
    """
    Helps to handle an SQLite database file
    """
    logging.basicConfig(level=logging.DEBUG)

    # static
    db_path: str = None  # contains the location of the database (.db) file
    GET_TABLES: str = "get tables"  # get all tables in db
    GET_MASTER: str = "get master"  # get all information stored in the db master table
    RAM_DB: str = ":memory:"  # create & store the database in RAM, pass as db path

    delete_table = lambda tablename: SQLite_handler.exec_all(SQLite_handler.db_path, f"DROP TABLE {tablename};")

    def __init__(self, db_path):
        SQLite_handler.db_path = db_path

    def __str__(self):
        print("sqlite handler")

    @staticmethod
    def exec_all(db_path: str=db_path, *args: str, **kwargs):
        """
        Executes all the given SQL queries (command strings) & prints the results to the console
        :param db_path :type str: location of the database in memory, path of the database (.db) file
        :param args :type list:
        :param kwargs:
        :return :type None
        """
        connection: sqlite3.Connection = sqlite3.connect(db_path)
        counter: int = 0
        for query in args:
            counter += 1  # count number of queries
            # check for special requests
            if query == SQLite_handler.GET_TABLES:
                query = "select name from sqlite_master where type='table'"
            elif query == SQLite_handler.GET_MASTER:
                query = "select * from sqlite_master"
            cursor: sqlite3.Cursor = connection.execute(query)
            results: list = cursor.fetchall()
            if results:
                print(results)
                for row in results:
                    logging.debug(row)
            else:
                print(f"query #{counter} executed. no output")
            connection.commit()
        connection.close()

    @staticmethod
    def get_table(tablename: str, filters: str=None, db_path: str=db_path) -> list:
        """
        read SQLite file and parse it to a matrix
        returns a list of all the fields in the table
        :param tablename :type str: name of desired table
        :param filters :type str: additional sql commands (WHERE)
        :param db_path :type str: location of the database in memory, path of the database (.db) file
        :return: list, all the fields in the table
        """
        database: list = []
        try:
            conn = sqlite3.connect(db_path)  # open db
            query = "SELECT * FROM {} {};".format(tablename, filters)
            cursor = conn.execute(query)  # SELECT * FROM table filters
            rows = cursor.fetchall()
            for row in rows:
                database.append(row)
            conn.close()
        except sqlite3.Error as e:
            logging.log(logging.ERROR, "failed to connect to SQLite DB: "
                                   "sqlite Error\n\rPlease check your SQL command syntax")
            raise e
        except Exception as e:
            logging.log(logging.ERROR, "failed to connect to SQLite DB")
            raise
        return database

    @staticmethod
    def create_log(level: int=logging.DEBUG):
        logging.basicConfig(level=level)
        logger = logging.getLogger()
        # logger.setLevel(level=logging.WARNING)
        return logger

    # : clean
    @staticmethod
    def sqlite_insert_table(matrix: list, db_path: str=db_path, table_schema: str=None, *args: str, **kwargs) -> int:
        """
        Write a given matrix to SQLite db as a table
        If the given table doesn't exist in the db, then create it according to given schema
        :param matrix :type list: the matrix that holds the data of the inserted table
        :param db_path :type str: location of the database in memory, path of the database (.db) file
        :param table_schema :type str: an SQL query containing the table schema (details)
        :param tablename :type str: name of the newly created table, defaults to 'default_table'
        :param args :type list
        :param kwargs:
        :return :type int: 0 if all is good
        """
        tablename: str = kwargs.get("tablename", None)
        if tablename is None: tablename = "default_table"  # default value
        try:
            conn = sqlite3.connect(db_path)  # open db
            # create table
            try:
                query = "CREATE TABLE %s (%s);" % (tablename, table_schema)  # TODO: generate automatic schema
                logging.debug(query)
                cursor = conn.execute(query)
            except sqlite3.OperationalError:
                logging.error("Couldn't create table %s. The table already exists" % tablename)
            except Exception:
                logging.error("Failed to create table " + tablename)
                raise
            # insert matrix into table
            for row in matrix:
                query = f"INSERT INTO {tablename} VALUES("  # : change to general query
                # if None in row:
                #     continue
                # insert the first element
                if type(row[0]) is str:
                    query += f"'{row[0]}'"
                elif row[0] is None:
                    query += "NULL"
                else:
                    query += f"{row[0]}"
                row = row[1:]  # pop the first element out
                # insert all other elements
                for item in row:
                    if type(item) is str:
                        query += f", '{item}'"
                    elif item is None:
                        query += f", NULL"
                    else:
                        query += f", {item}"
                query += ");"
                # insert the row into the table
                logging.debug(query)
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

    @staticmethod
    def print_2D_matrix(table: list) -> None:
        """
        Prints any table (matrix) to the console in a nice format
        :param table :type list: the table to be printed
        :return :type None
        """
        for row in table:
            print_string: str = "| "
            if type(row) in (list, dict):
                for value in row:
                    print_string += str(value) + " | "
            else: print_string += str(row) + " | "
            print(print_string)

    @staticmethod
    def create_table(db_path: str, table_schema: str=None, tablename: str="default_table", *args: str, **kwargs) -> int:
        """
        Create an SQLite table according to a given schema
        :param db_path: :type str: location of the database in memory, path of the database (.db) file
        :param table_schema: :type str: an SQL query containing the table schema (details)
        :param tablename: :type str: name of the newly created table, defaults to 'default_table'
        :param args :type list
        :param kwargs:
        :return :type int: 0 if all is good
        """
        try:
            conn = sqlite3.connect(db_path)  # open db
            # create table
            try:
                query = "CREATE TABLE %s (%s);" % (tablename, table_schema)  # TODO: generate automatic schema
                logging.debug(query)
                cursor = conn.execute(query)
            except sqlite3.OperationalError as e:
                logging.error("Couldn't create table %s. The table already exists" % tablename)
                logging.error(e)
            except Exception as e:
                logging.error("Failed to create table " + tablename)
                logging.error(e)
                raise

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

    @staticmethod
    def insert_json(json: Union[dict, list], tablename: str, db_path: str=db_path, special_chars: bool=True):
        """
        Insert a json object into the given table
        :param json:
        :return:
        """
        if type(json) is dict:
            # SQLite_handler.insert_dictionary(tablename=tablename, json=json, db_path=db_path)
            SQLite_handler.insert_dictionary_abstract(tablename=tablename, json=json, db_path=db_path,
                                                      special_chars=special_chars)
        elif type(json) is list:
            for item in json:
                SQLite_handler.insert_json(db_path=db_path, json=item, tablename=tablename)

    @staticmethod
    def insert_dictionary(json, tablename: str, db_path: str=db_path):
        """
        Insert a given dictionary to sqlite according to the columns' names
        :param json:
        :param tablename:
        :param db_path:
        :return:
        """
        # SQLite_handler.exec_all(db_path, "PRAGMA table_info(vehicles);")
        schema: List[Tuple[int, str, str, int, None, int]] = \
            SQLite_handler.get_schema(tablename=tablename, db_path=db_path)
        # print(ret)
        column_list: List[str] = list()
        for column in schema:
            column_list.append(column[1])
        # print(column_list)
        # print(json)
        values: str = ''
        for column in column_list:
            column = column.replace("_", " ")
            column = column.replace("\"", "")
            column = column.replace("\'", "")
            try:
                string = json[column].replace("\"", "")
                string = string.replace("\'", "")
                # INSERT INTO vehicles VALUES("2008 BMW ב.מ.וו F800GS", "₪45,000", ...);
                # INSERT INTO vehicles (model, price) VALUES("2008 BMW ב.מ.וו F800GS", "₪45,000");
                values += rf'"{string}", '
            except Exception as e:
                # logging.error(e)
                logging.error(f"no such column {column}")
                values += rf'NULL, '
        values = values[:-2]  # pop last ,
        # print(values)
        try:
            SQLite_handler.exec_all(db_path, f"INSERT INTO {tablename} VALUES({values});")
        except sqlite3.OperationalError as e:
            logging.error("syntax error")
            print(f"INSERT INTO {tablename} VALUES({values});")
            raise e
        except Exception as e:
            print(f"INSERT INTO {tablename} VALUES({values});")
            raise e

    @staticmethod
    def get_schema(tablename: str, db_path: str=db_path, *args: Tuple[Any], **kwargs: Dict[Any, Any]):
        """
        Get the schema of the table
        :param tablename:
        :param db_path:
        :param args:
        :param kwargs:
        :return:
        """
        connection: sqlite3.Connection = sqlite3.connect(db_path)
        cursor: sqlite3.Cursor = connection.execute(f"PRAGMA table_info({tablename});")
        results: list = cursor.fetchall()
        # if results:
        #     # print(results)
        #     for row in results:
        #         logging.debug(row)
        # else:
        #     raise Exception(f"cant get table {tablename} schema")
        connection.commit()
        connection.close()
        return results

    @staticmethod
    def insert_dictionary_abstract(json, tablename: str, db_path: str = db_path, special_chars: bool=True):
        """
        Insert a given dictionary to sqlite according to the columns' names
        :param json:
        :param tablename:
        :param db_path:
        :return:
        """
        # SQLite_handler.exec_all(db_path, f"PRAGMA table_info({tablename});")
        schema: List[Tuple[Any]] = SQLite_handler.get_schema(tablename=tablename, db_path=db_path)
        # print(ret)
        column_list: List[str] = list()
        for column in schema:
            column_list.append(column[1])
        # print(column_list)
        # print(json)
        values: str = ''
        for column in column_list:
            if special_chars:
                column = column.replace("_", " ")
                column = column.replace("\"", "")
                column = column.replace("\'", "")
            try:
                string = json[column].replace("\"", "")
                string = string.replace("\'", "")
                # INSERT INTO vehicles VALUES("2008 BMW ב.מ.וו F800GS", "₪45,000", ...);
                # INSERT INTO vehicles (model, price) VALUES("2008 BMW ב.מ.וו F800GS", "₪45,000");
                values += rf'"{string}", '
            except Exception as e:
                # logging.error(e)
                logging.error(f"no such column {column}")
                values += rf'NULL, '
        values = values[:-2]  # pop last ,
        print(values)
        try:
            SQLite_handler.exec_all(db_path, f"INSERT INTO {tablename} VALUES({values});")
        except sqlite3.OperationalError as e:
            logging.error("syntax error")
            print(f"INSERT INTO {tablename} VALUES({values});")
            raise e
        except Exception as e:
            print(f"INSERT INTO {tablename} VALUES({values});")
            raise e
