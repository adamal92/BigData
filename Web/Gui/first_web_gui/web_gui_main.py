import logging

import requests
from flask import Flask, render_template
from flaskwebgui import FlaskUI   # get the FlaskUI class
from requests import Response

from NoSQL.ElasticSearch.elasticsearch_handler import Elasticsearch_Handler
from testsAndOthers.data_types_and_structures import DataTypesHandler

app = Flask(__name__)

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
    from Web.Gui.first_web_gui.crawler_mini_proj_using_libs import main as start
    start()  # main()
    return render_template('crawler.html')


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
        # return {"name": "ok"}  # TODO: return the json from elastic
        return json_dict

    except ConnectionError:
        return render_template('Error.html')


# TODO: get data from db (sqlite/elastic) & visualise it at client side (js/kibana)
if __name__ == '__main__':
    logging.getLogger('flaskwebgui').setLevel(logging.ERROR)
    logging.getLogger('BaseHTTPRequestHandler').setLevel(logging.ERROR)
    ui.run()                           # call the 'run' method
