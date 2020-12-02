import string, random, sqlite3, time
from SQL.SQLite_database_handler import SQLite_handler
from concurrent.futures.process import ProcessPoolExecutor


def get_random_string(length: int = 10) -> str:
    letters: str = string.ascii_lowercase
    result_str: str = ''.join(random.choice(letters) for i in range(length))
    # logging.debug(f"Random string of length {length} is: {result_str}")
    return result_str


def append_async(lst: list):
    ret: list = []
    for index in lst:
        ret.append((index, random.randint(-100, 100), get_random_string(1000)))
        # print(index)
    return ret


def create_random_matrix_async(magnitude: int, chunksize: int) -> list:
    """

    :param chunksize :type int:
    :param magnitude :type int:
    :return:
    """
    mtrx: list = []

    rang_e = range(0, 10 ** magnitude + 1, 1)
    lst_ranges = [rang_e[i: i + chunksize] for i in range(0, len(rang_e), chunksize)]
    print(lst_ranges)

    for lst in lst_ranges:
        with ProcessPoolExecutor() as executor:
            future = executor.submit(append_async, lst)
            for result in future.result():
                mtrx.append(result)
    return mtrx


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


def create_random_table2(magnitude: int=3, chunksize: int=100, asynch: bool=False) -> float:
    print(SQLite_handler.db_path)
    schema: str = "numIndex INT, randomNum INT, randomNames VARCHAR(1000)"
    print(SQLite_handler.create_table(SQLite_handler.db_path, table_schema=schema, tablename="random"))

    start_create = time.time()
    if asynch: examples = create_random_matrix_async(magnitude=magnitude, chunksize=chunksize)
    else: examples = create_random_matrix(magnitude=magnitude)
    stop_create = time.time()

    connection: sqlite3.Connection = sqlite3.connect(SQLite_handler.db_path)
    cur = connection.cursor()
    print("CREATING TABLE")
    SQLite_handler.print_2D_matrix(table=examples)
    print(examples)
    cur.executemany("INSERT INTO random VALUES(?, ?, ?)", examples)
    connection.commit()
    connection.close()

    return stop_create - start_create


def main():
    start = time.time()

    sqlithndlr: SQLite_handler = \
        SQLite_handler(db_path=r"C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db")
    SQLite_handler.exec_all(SQLite_handler.db_path, "DROP TABLE random;")
    create_time: float = create_random_table2(magnitude=4, asynch=True)
    SQLite_handler.exec_all(SQLite_handler.db_path, "SELECT COUNT(*) FROM random;")

    print(time.time() - start)
    print(create_time)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
