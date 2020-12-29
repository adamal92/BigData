import logging
import os
import signal
import subprocess
import sys
import time
from subprocess import Popen

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

    def __init__(self):
        self._elastic_process = self.start_search()
        self._kibana_process = Elasticsearch_Handler.start_kibana(self)
        os.system("start http://localhost:9200/")  # search
        os.system("start http://localhost:5601/")  # kibana

    def start_search(self) -> Popen:
        self._elastic_process: Popen = subprocess\
            .Popen([sys.executable, f'{os.getcwd()}\\start_search.py'], stdout=sys.stdout, shell=True,
                   creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)  # search
        # .Popen(["python", f'{os.getcwd()}\\start_search.py'], stdout=sys.stdout)  # search
        # p.communicate()  # wait for process to end
        print(type(self._elastic_process))
        time.sleep(15)  # minimum time that elasticsearch takes to start: 13
        return self._elastic_process

    def start_kibana(self) -> Popen:
        self._kibana_process = subprocess\
            .Popen(["python", f'{os.getcwd()}\\start_kibana.py'], stdout=sys.stdout)  # kibana
        # p.communicate()  # wait for process to end
        time.sleep(30)  # minimum time that kibana takes to start:
        return self._kibana_process

    def stop_search(self):
        self._elastic_process.kill()
        self._elastic_process.terminate()
        # os.kill(self._elastic_process.pid, signal.SIGTERM)  # or signal.SIGKILL, needs admin premissions
        # os.killpg(self._elastic_process.pid, signal.SIGTERM)  # Send the signal to all the process groups
        os.kill(self._elastic_process.pid, signal.CTRL_BREAK_EVENT)  # CTRL_BREAK_EVENT, CTRL_C_EVENT

    def stop_kibana(self):
        self._kibana_process.terminate()
        self._kibana_process.kill()
        # os.kill(self._kibana_process.pid, signal.SIGTERM)  # or signal.SIGKILL
        # os.killpg(os.getpgid(pro.pid), signal.SIGTERM)  # Send the signal to all the process groups
        # os.killpg(self._kibana_process.pid, signal.SIGTERM)  # Send the signal to all the process groups

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
        :param print_form :type int, one of the DataTypesHandler.PRINT constants:
        :param additional_args :type bool: True if additional arguments are passed to the function as *args or **kwargs
        :param print_recursively :type bool: determines how the json is printed to the console
        :param fn :type function: a function that gets a url string and returns a requests.Response object containing a json file
        :param url :type str: the url that gets passed to the given function
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
        :param url :type str: the url that gets passed to the given function
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

    @staticmethod
    def send_request(fn, max_tries: int, url: str=DEFAULT_URL, print_recursively: bool=False,
             additional_args: bool=False, print_form: int=DataTypesHandler.PRINT_ARROWS,
                      *args, **kwargs) -> Response:
        """
        Execute an ElasticSearch command (an http request).
        Call a given function with ElasticSearch url as its' argument & print the resulted json file (of the response),
        using the Elasticsearch_Handler.exec function
        :param max_tries :type int: max amount of requests that would be sent before raising a ConnectionError
        :param print_form :type int, one of the DataTypesHandler.PRINT constants:
        :param additional_args :type bool: True if additional arguments are passed to the function as *args or **kwargs
        :param print_recursively :type bool: determines how the json is printed to the console
        :param fn :type function: a function that gets a url string and returns a requests.Response object containing
        a json file
        :param url :type str: the url that gets passed to the given function
        :param args:
        :param kwargs:
        :return :type requests.Response: The received HTTP response
        """

        for counter in range(0, max_tries):
            try:
                return Elasticsearch_Handler.exec(fn=fn, url=url, print_recursively=print_recursively,
                                                  additional_args=additional_args, print_form=print_form)
            except:
                print("Connection refused by the server..")
                print("Let me sleep for 5 seconds")
                print(f"Attempt no. {counter+1}       ZZzzzz...")
                time.sleep(5)
                print("Was a nice sleep, now let me continue...")
                continue

        # if counter >= max_tries
        raise ConnectionError("Max tries reached, can't connect to Elastic")

