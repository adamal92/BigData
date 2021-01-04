"""
This is the moto_prices projects' entry point
"""

import logging
import time

import requests
from flask import Flask, render_template
from flaskwebgui import FlaskUI  # get the FlaskUI class
from requests import Response

from NoSQL.ElasticSearch.elasticsearch_handler import Elasticsearch_Handler
from testsAndOthers.data_types_and_structures import DataTypesHandler

app: Flask = Flask(__name__)

# Feed it the flask app instance
ui = FlaskUI(app)                 # feed the parameters


# do your logic as usual in Flask

@app.route("/")
def index():
    # return "It works!"
    return render_template('index.html')


@app.route("/home", methods=['GET'])
def home():
    return render_template('home.html')
    # return "ok"


@app.route("/start", methods=['GET'])
def start_crawler():
    from BD_projects.moto_prices.get_data import main as start
    start()  # main()
    return render_template('crawler.html')
    # return "ok"
# TODO: start elastic button


@app.route("/crawl", methods=['GET'])
def get_crawler_html():
    return render_template('crawler.html')


@app.route("/react", methods=['GET'])
def react():
    return render_template('react_app.html')


@app.route("/react_app.jsx", methods=['GET'])
def get_like():
    return open("static/react_app.jsx", "r").read()


@app.route("/react/json", methods=['GET'])
def react_json():
    return render_template('react_json.html')


@app.route("/react/react_json.js", methods=['GET'])
def get_json_react():
    return open("static/react_json.js", "r").read()


@app.route("/get_json")
def get_json():
    # return "It works!"
    # get table from elastic
    try:
        response: Response = Elasticsearch_Handler.send_request(fn=lambda url: requests.get(url + "school/_doc/quotes"),
                                                                print_recursively=True,
                                                                print_form=DataTypesHandler.PRINT_DICT, max_tries=5)
        logging.warning(response.json()["_source"])
        json_dict: dict = response.json()["_source"]
        # names_list: list = DataTypesHandler.dict_to_matrix(dictionary=json_dict)
        # logging.warning(names_list)
        # return {"name": "ok"}  # : return the json from elastic
        # return "<p><br />"+str(json_dict)+"</p>"
        html = "<p><br />"+str(json_dict)+r'</p> <a href="\" class="button">Go back</a>'
        return html  # string, dict, tuple, Response instance, or WSGI callable

    except ConnectionError as e:
        logging.error(e)
        return render_template('Error.html')


@app.route("/chart", methods=['GET'])
def chart():
    return render_template("chart.html")


@app.route("/visualize_json", methods=['GET'])
def visualize_json():
    return render_template("visualize_json.html")


@app.route("/get_json_visualization", methods=['GET'])
def get_json_visualization():
    # get json from elastic
    try:
        logging.debug("get_json_visualization()")
        response: Response = Elasticsearch_Handler.send_request(fn=lambda url: requests.get(url + "school/_doc/quotes"),
                                                                print_recursively=True,
                                                                print_form=DataTypesHandler.PRINT_DICT, max_tries=3)
        logging.warning(response.json()["_source"])
        json_dict: dict = response.json()["_source"]
        return json_dict  # string, dict, tuple, Response instance, or WSGI callable

    except ConnectionError as e:
        logging.error(e)
        raise e
        # return render_template('Error.html')


@app.route("/crawl_motorcycles", methods=['GET'])
def moto_crawler_html():
    return render_template('moto_crawler.html')


@app.route("/moto_spider", methods=['GET'])
def moto_spider_html():
    from BD_projects.moto_prices.moto_crawler import MotoCrawler
    MotoCrawler.get_file()
    return render_template('moto_crawler.html')


@app.route("/moto_hdfs", methods=['GET'])
def moto_hdfs():
    # hdfs & spark
    from BD_projects.moto_prices.moto_crawler import MotoCrawler
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.start()
    MotoCrawler.save_file_to_hdfs(file_path=MotoCrawler.motorcycle_file.name,
                                  filename=MotoCrawler.motorcycle_file.name)
    time.sleep(2)
    # json_count_names: dict = Spark_handler.pass_to_spark(
    #     file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}user/hduser/quotes.jl", process_fn=process_data
    # )
    HDFS_handler.stop()
    return render_template('moto_crawler.html')


@app.route("/moto_spark", methods=['GET'])
def moto_spark():
    st = time.time()

    from BD_projects.moto_prices.moto_crawler import MotoCrawler
    from Hadoop.hdfs import HDFS_handler
    from Spark.Spark_handler_class import Spark_handler
    import os
    HDFS_handler.start()
    os.system("hdfs dfsadmin -safemode leave")  # safe mode off
    time.sleep(2)

    json_count_names: dict = Spark_handler.pass_to_spark(
        file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}user/hduser/{MotoCrawler.motorcycle_file.name}",
        process_fn=MotoCrawler.process_data
    )

    os.system("hdfs dfsadmin -safemode enter")  # safe mode on
    HDFS_handler.stop()

    from testsAndOthers.data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(data=json_count_names)
    logging.debug(f"spark total time: {time.time() - st} seconds")

    # # elastic
    # TODO: upload_json_to_elastic(json=json_count_names)
    return render_template('moto_crawler.html')


@app.route("/crawl_motorcycles_dirty", methods=['GET'])
def crawl_motorcycles_dirty():
    from BD_projects.moto_prices.moto_crawler import MotoCrawler
    MotoCrawler.start_scrapy_spider(save_as="moto_dirty.json",
                                    spider_py=r'BigData\BD_projects\moto_prices\dirty_spider_2.py',
                                    delimeter=True, dirs_till_root=3)
    return render_template('index.html')


# : get data from db (sqlite/elastic) & visualise it at client side (js/kibana)
if __name__ == '__main__':
    logging.getLogger('flaskwebgui').setLevel(logging.ERROR)
    logging.getLogger('BaseHTTPRequestHandler').setLevel(logging.ERROR)
    logging.getLogger('matplotlib').setLevel(logging.ERROR)
    logging.getLogger('py4j').setLevel(logging.ERROR)
    logging.getLogger('my_log').setLevel(logging.DEBUG)

    ui.run()                           # call the 'run' method
