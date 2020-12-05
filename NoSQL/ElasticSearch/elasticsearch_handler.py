import logging
import requests
from requests import Response

from SQL.SQLite_database_handler import SQLite_handler
from testsAndOthers.data_types_and_structures import DataTypesHandler


class Elasticsearch_Handler:
    """
    Helps manipulating & executing ElasticSearch commands
    """
    # static
    DEFAULT_URL: str = 'http://localhost:9200/'  # address of the default elastic search server

    @staticmethod
    def print_dict(dictionary: dict):
        """
        prints recursively a dictionary in a nice format
        :param dictionary :type dict: the dictionary to be printed
        :return :type None
        """
        for key in dictionary.keys():
            value = dictionary[key]
            if type(value) is dict:
                print("%s: {" % key)
                Elasticsearch_Handler.print_dict(value)
                print("}")
            elif type(value) is list:
                DataTypesHandler.print_2D_matrix(dictionary[key])
                # SQLite_handler.print_2D_matrix(dictionary[key])
            else:
                print(f"{key}: {value}")

    @staticmethod
    def exec(fn, url: str=DEFAULT_URL, print_recursively: bool=False,
             additional_args: bool=False, print_form: int=DataTypesHandler.PRINT_ARROWS, *args, **kwargs) -> Response:
        """
        Execute an ElasticSearch command.
        Call a given function with ElasticSearch url as its' argument & print the resulted json file (of the response)
        :param print_form:
        :param additional_args :type bool: True if additional arguments are passed to the function as *args or **kwargs
        :param print_recursively :type bool: determines how the json is printed to the console
        :param fn: a function that gets a url string and returns a requests.Response object containing a json file
        :param url: the url that gets passed to the given function
        :param args:
        :param kwargs:
        :return :type requests.Response: The received HTTP response
        """
        try:
            if additional_args:
                resp: Response = fn(url, *args, **kwargs)
            else:
                resp: Response = fn(url)
        except requests.exceptions.ConnectionError as e:
            logging.error("Please check the provided url & that the server is running")
            raise e
        except Exception as e:
            raise e

        if resp.status_code != 200:
            # This means something went wrong.
            logging.error(Exception('status code: {}'.format(resp.status_code)))

        # print(type(resp.json()))
        if print_recursively: print(DataTypesHandler.print_data_recursively(data=resp.json(), print_dict=print_form))
        else: print(DataTypesHandler.print_dict(resp.json()))

        return resp

    @staticmethod
    def sqlite_upload_table_to_elasticsearch(url: str, tablename: str, db_path: str,
                                             filters: str=None, class_print: bool=False, schema: list=None,
                                             print_form: int = DataTypesHandler.PRINT_ARROWS,
                                             *args, **kwargs) -> requests.Response:
        """
        Retrieve an SQLite table from database and create a copy of it in elastic search using json format
        :param url: the url that gets passed to the given function
        :param tablename :type str: name of desired sqlite table
        :param db_path :type str: location of the database in memory, path of the database (.db) file
        :param filters :type str: additional sql commands
        :param class_print :type bool: determines if <class > bracket will be printed
        :param schema :type list[str]: contains all the column names of the table, ordered
        :param print_form :type int, one of the DataTypesHandler.PRINT constants:
        determines in which way the dictionaries would be printed
        :param args:
        :param kwargs:
        :return :type requests.Response: The received HTTP response
        """
        # get table from sqlite
        table: list = SQLite_handler.get_table(tablename=tablename, filters=filters, db_path=db_path)
        # create table schema
        if not schema:
            schema = []
            for header in table[0]:
                if class_print:
                    schema.append(str(type(header)))
                else:
                    schema.append(str(type(header)).split("'")[1])
        logging.debug(f" schema: {len(schema)}")
        # create dictionary (json)
        dictionary_json = DataTypesHandler.matrix_to_dict(matrix=table, schema=schema)
        logging.debug(f" dictionary: {dictionary_json}")

        # the passed function
        def pass_func(url: str, *args, **kwargs) -> Response:
            dict_json: dict = kwargs.get("dict_json", None)
            return requests.post(url=url, json=dict_json)

        # send table to elastic search
        resp: Response = Elasticsearch_Handler.exec(fn=pass_func, print_recursively=True, additional_args=True,
                                   url=url, dict_json=dictionary_json, print_form=print_form)
        # print the created elastic search table
        Elasticsearch_Handler.exec(fn=lambda url: requests.get(url), print_recursively=True,
                                   url=url, print_form=print_form)
        return resp
