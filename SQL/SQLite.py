import sqlite3

# global
database = []

# ":memory:" as db name for creating & storing it in RAM
# open db
conn: sqlite3.Connection = sqlite3.connect(r'C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db')
cursor: sqlite3.Cursor = conn.execute("SELECT * FROM employee;")  # run db

for row in cursor:
    for column in row:
        print(column)
        database.append(column)


conn.close()

print(type(conn), isinstance(conn, str))
print(cursor)
print(database)


class SQLite_DB:
    @staticmethod
    def get_table(tablename: str, filters: str) -> list:
        """
        read SQLite file and parse it to a matrix
        returns a list of all the fields in the table
        :param tablename :type str: name of desired table
        :param filters :type str: additional sql commands (WHERE)
        :return: list, all the fields in the table
        """
        database: list = []
        try:
            conn = sqlite3.connect(r'C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db')  # open db
            query = "SELECT * FROM {} {};".format(tablename, filters)
            cursor = conn.execute(query)  # SELECT * FROM table filters
            rows = cursor.fetchall()
            for row in rows:
                database.append(row)
            conn.close()
        except sqlite3.Error as e:
            print("failed to connect to SQLite DB")
            raise e
        except Exception as e:
            print("failed to connect to SQLite DB")
            raise
        return database


print(SQLite_DB.get_table("branch", None))

for row in SQLite_DB.get_table("branch", None):
    print(row)


from SQL.SQLite_database_handler import SQLite_handler
import time

sql_time = time.time()

sqlithndlr: SQLite_handler = \
    SQLite_handler(db_path=r"C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db")
# SQLite_handler.exec_all(SQLite_handler.db_path, "CREATE INDEX ind ON branch(branch_name);")
# SQLite_handler.exec_all(SQLite_handler.db_path, "SELECT COUNT(*) FROM branch")

# insert/create table random
import random, string


def get_random_string(length: int=10) -> str:
    letters: str = string.ascii_lowercase
    result_str: str = ''.join(random.choice(letters) for i in range(length))
    # logging.debug(f"Random string of length {length} is: {result_str}")
    return result_str


def create_random_table():
    mtrx: list = []
    for index in range(0, 10 ** 8 + 1, 1):
        mtrx.append([index, random.randint(-100, 100), get_random_string(100)])
    schema: str = "index INT, randomNum INT, randomNames VARCHAR(1000)"
    print(SQLite_handler.sqlite_insert_table(tablename="random", table_schema=schema, matrix=mtrx,
                                             db_path=sqlithndlr.db_path))
    table: list = SQLite_handler.get_table(tablename="random", db_path=sqlithndlr.db_path)
    SQLite_handler.print_2D_matrix(table)


def create_random_matrix(magnitude: int) -> list:
    """

    :param magnitude :type int:
    :return:
    """
    mtrx: list = []
    for index in range(0, 10 ** magnitude + 1, 1):
        mtrx.append((index, random.randint(-100, 100), get_random_string(1000)))
        print(index)
    return mtrx


from concurrent.futures.process import ProcessPoolExecutor

# mtrx: list = []


def append_async(lst: list):
    ret: list = []
    for index in lst:
        ret.append((index, random.randint(-100, 100), get_random_string(1000)))
        # print(index)
    return ret


def create_random_matrix_async(magnitude: int) -> list:
    """

    :param magnitude :type int:
    :return:
    """
    mtrx: list = []
    # for index in range(0, 10 ** magnitude + 1, 1):
    #     mtrx.append((index, random.randint(-100, 100), get_random_string(1000)))
    #     print(index)

    rang_e = range(0, 10 ** magnitude + 1, 1)
    chunksize = pow(magnitude, 5)
    lst_ranges = [rang_e[i: i+chunksize] for i in range(0, len(rang_e), chunksize)]
    print(lst_ranges)

    for lst in lst_ranges:
        with ProcessPoolExecutor() as executor:
            future = executor.submit(append_async, lst)
            for result in future.result():
                mtrx.append(result)
    return mtrx


    # with ProcessPoolExecutor() as executor:
    #     future = executor.map(mtrx.append,
    #     for index in range(0, 10 ** magnitude + 1, 1): (index, random.randint(-100, 100), get_random_string(1000)))
    #     print(future.result())


def create_random_table2():
    print(SQLite_handler.db_path)
    schema: str = "numIndex INT, randomNum INT, randomNames VARCHAR(1000)"
    print(SQLite_handler.create_table(SQLite_handler.db_path, table_schema=schema, tablename="random"))
    # time.sleep(5)
    examples = create_random_matrix_async(magnitude=3)  # 74.05134510993958 (5), 12.9612560272216 (4), 2.0253500938415527 (3)
    # examples = create_random_matrix(magnitude=3)  # 62.527122020721436 (5), 6.2024383544921875 (4), 1.1457383632659912 (3)
    connection: sqlite3.Connection = sqlite3.connect(SQLite_handler.db_path)
    cur = connection.cursor()
    print("CREATING TABLE")
    SQLite_handler.print_2D_matrix(table=examples)
    print(examples)
    cur.executemany("INSERT INTO random VALUES(?, ?, ?)", examples)
    connection.commit()
    connection.close()


if __name__ == '__main__':
    start = time.time()

    # import tkinter, gui
    # root = tkinter.Tk()
    # app = gui.Gui(master=root)
    # app.mainloop()

    SQLite_handler.exec_all(SQLite_handler.db_path, "DROP TABLE random;")
    create_random_table2()
    SQLite_handler.exec_all(SQLite_handler.db_path, "SELECT COUNT(*) FROM random;")

    # time.sleep(5)
    print(time.time() - start, time.time() - sql_time)

    # 4.01999306678772
    # 2.9290530681610107
