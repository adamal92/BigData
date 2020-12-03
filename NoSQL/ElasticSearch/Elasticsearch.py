import requests, logging, os, sys
from requests import Response

from NoSQL.ElasticSearch.elasticsearch_handler import Elasticsearch_Handler
from SQL.SQLite_database_handler import SQLite_handler

"""
Run elasticsearch commands
"""

# os.system("start http://localhost:5601/")  # kibana
# os.system("start http://localhost:9200/")  # search

Elasticsearch_Handler.exec(fn=lambda url: requests.get(url))
Elasticsearch_Handler.exec(fn=lambda url: requests.get(url + "_search"))

Elasticsearch_Handler.exec(fn=lambda url: requests.put(url + "school"))
Elasticsearch_Handler.exec(fn=lambda url: requests.get(url + "school/_search"))


def upload_json_to_elasticsearch(url: str) -> Response:
    json = {
       "name":"Saint Paul School", "description":"ICSE Afiliation",
       "street":"Dawarka", "city":"Delhi", "state":"Delhi", "zip":"110075",
       "location":[28.5733056, 77.0122136], "fees":5000,
       "tags":["Good Faculty", "Great Sports"], "rating":"4.5"
    }

    return requests.post(url=url + "school/_doc/10", json=json)


Elasticsearch_Handler.exec(fn=upload_json_to_elasticsearch, print_recursively=True)

Elasticsearch_Handler.exec(fn=lambda url: requests.get(url + "school/_doc/10"), print_recursively=True)

Elasticsearch_Handler.exec(fn=lambda url: requests.delete(url + "school"))

Elasticsearch_Handler.exec(fn=lambda url: requests
                           .get(url + "kibana_sample_data_ecommerce/_doc/ciD7_nUBXOsJjDvfmi7d"), print_recursively=True)

Elasticsearch_Handler.exec(fn=lambda url: requests
                           .get(url + "schools_pri*/_search?allow_no_indices=true"), print_recursively=True)

Elasticsearch_Handler.exec(fn=lambda url: requests.get(url), print_recursively=True)


# : move upload_table_to_elasticsearch() to Elasticsearch_Handler
from testsAndOthers.data_types_and_structures import DataTypesHandler

def upload_table_to_elasticsearch(url: str, *args, **kwargs) -> Response:
    tablename: str = kwargs.get("tablename", "default_table")
    db_path: str = kwargs.get("db_path", None)
    filters: str = kwargs.get("filters", None)
    schema: list = kwargs.get("schema", None)
    class_print: bool = kwargs.get("class_print", False)

    table: list = SQLite_handler.get_table(tablename=tablename, filters=filters, db_path=db_path)

    if not schema:
        schema = []
        for header in table[0]:
            if class_print:
                schema.append(str(type(header)))
            else:
                schema.append(str(type(header)).split("'")[1])

    dictionary_json = DataTypesHandler.matrix_to_dict(matrix=table, schema=schema)  # ["num1", "name", "num2", "date"]
    print(dictionary_json)
    return requests.post(url=url, json=dictionary_json)


Elasticsearch_Handler.exec(fn=upload_table_to_elasticsearch,
                           print_recursively=True, additional_args=True,
                           url=Elasticsearch_Handler.DEFAULT_URL+"school/_doc/branch",
                           tablename="branch",
                           db_path=r'C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db')

Elasticsearch_Handler.exec(fn=lambda url: requests.get(url+"school/_doc/branch"), print_recursively=False)


Elasticsearch_Handler.sqlite_upload_table_to_elasticsearch(
    url=Elasticsearch_Handler.DEFAULT_URL+"school/_doc/branch", tablename="branch",
    db_path=r'C:\cyber\PortableApps\SQLiteDatabaseBrowserPortable\first_sqlite_db.db', class_print=True,
    filters=None, print_form=DataTypesHandler.PRINT_ARROWS
)

print("-----------------------------------------")
json = {
    "mappings": {
        "products": {
            "properties": {
                "name": {
                    "type": "string"
                },
                "price": {
                    "type": "double"
                },
                "description": {
                    "type": "string"
                },
                "status": {
                    "type": "string"
                },
                "quantity": {
                    "type": "integer"
                },
                "categories": {
                    "type": "nested",
                    "properties": {
                        "name": {
                            "type": "string"
                        }
                    }
                },
                "tags": {
                    "type": "string"
                }
            }
        }
    }
}
Elasticsearch_Handler.exec(fn=lambda url: requests.put(url=url+"utubecommerce/", json=json), print_recursively=True,
                           print_form=DataTypesHandler.PRINT_DICT)

print("-----------------------------------------")

json: dict = {
   "query": {
      "query_string": {
         "query": "any_string"
      }
   }
}

Elasticsearch_Handler.exec(fn=lambda url: requests.post(url+"school/_all/_search", json=json), print_recursively=True,
                           print_form=DataTypesHandler.PRINT_DICT)

# try:
#     # resp = requests.get(URL)
#     # resp = requests.get(URL + "_search")
#     # resp = requests.get(URL + "school/_search")
#     # resp = requests.put(URL + "school")
#     json = {
#    "name":"Saint Paul School", "description":"ICSE Afiliation",
#    "street":"Dawarka", "city":"Delhi", "state":"Delhi", "zip":"110075",
#    "location":[28.5733056, 77.0122136], "fees":5000,
#    "tags":["Good Faculty", "Great Sports"], "rating":"4.5"
#     }
#     resp: Response = requests.post(url=URL+"school/_doc/10", json=json)
#     # resp = requests.delete(URL + "school")
# except requests.exceptions.ConnectionError as e:
#     logging.error("Please check the provided url & that the server is running")
#     raise e
# except Exception as e:
#     raise e
#
# if resp.status_code != 200:
#     # This means something went wrong.
#     logging.error(Exception('status code: {}'.format(resp.status_code)))
#
#
# json_dict: dict = resp.json()
# print(type(resp.json()))
# print(Elasticsearch_Handler.print_dict(resp.json()))
#
# import os, sys, subprocess
# import admin

#
# def test():
#     rc = 0
#
#     if not admin.isUserAdmin():
#         print("You're not an admin.", os.getpid(), "params: ", sys.argv)
#         rc = admin.runAsAdmin()  # cmdLine=[sys.executable] + sys.argv
#
#     else:
#         print("You are an admin!", os.getpid(), "params: ", sys.argv)
#         rc = 0
#         # os.system("elasticsearch.bat")
#         print("helll")
#         # os.system("kibana.bat")
#         # p = subprocess.Popen(["elasticsearch.bat"], stdout=sys.stdout)
#         # p.communicate()
#     x = input('Press Enter to exit.')
#
#     return rc
#
#
# if __name__ == "__main__":
#     sys.exit(test())

# os.system(f"\"{sys.executable}\" \"{os.getcwd()}\\start_kibana.py\"")
