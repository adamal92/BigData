import sqlite3

DATABASE_PATH = r"C:\Users\Sveta\Desktop\mydb.db"


def create_db():
    connection: sqlite3.Connection = sqlite3.connect(DATABASE_PATH)
    cursor: sqlite3.Cursor = connection.execute("create table user(int grade); \n\rcreate table techer(int grade);")
    print(cursor.fetchall())
    connection.commit()
    connection.close()


def exec_all(*args, **kwargs):
    connection: sqlite3.Connection = sqlite3.connect(DATABASE_PATH)
    counter: int = 0
    for query in args:
        counter += 1
        cursor: sqlite3.Cursor = connection.execute(query)
        results: list = cursor.fetchall()
        if results:
            print(results)
        else:
            print(f"query #{counter} executed. no output")
        connection.commit()
    connection.close()


with open(DATABASE_PATH, "w+"):
    connection: sqlite3.Connection = sqlite3.connect(DATABASE_PATH)
    cursor: sqlite3.Cursor = connection.execute("create table student(int grade);")
    print(connection.execute("insert into student values(9);").fetchall())
    print(connection.execute("select name from sqlite_master where type='table'").fetchall())  # get all tables from db
    print(connection.execute("select * from sqlite_master").fetchall())  # get all tables from db
    connection.commit()
    connection.close()

    # create_db()
    exec_all("insert into student values(9);",
             "insert into student values(10);",
             "insert into student values(9);",
             "select * from student;"
             "",
             "DROP TABLE student;",
             "CREATE TABLE chat(mssg_id INT, first_name VARCHAR(40),"
             " last_name VARCHAR(40), birth_day DATE, sex VARCHAR(1),"
             " salary INT, super_id INT, branch_id INT);",
             ""
             "INSERT INTO chat VALUES();",
             "SELECT * FROM chat;",
             "select name from sqlite_master where type='table';",
             "DROP TABLE chat;")

