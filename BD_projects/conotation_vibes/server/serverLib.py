# -*- coding: utf-8 -*-
import logging
import os
import subprocess
import sys
import time
from subprocess import Popen
from typing import List, Dict

from firebase import Firebase
# pip install python_jwt
# pip install gcloud
# pip install sseclient
# pip install pycrypto
# pip install requests-toolbelt
from apscheduler.schedulers.blocking import BlockingScheduler


class Constants:
    db = {}
    # where: filter by sql query, outFields: filter columns, f: format style
    API_URL = 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/' \
              'ncov_cases2_v1/FeatureServer/2/query?' \
              'where=Country_Region = \'israel\'' \
              '&outFields=Country_Region, Last_Update, Confirmed, Deaths, Recovered, Active, Mortality_Rate' \
              '&outSR=4326' \
              '&f=json' \
        # get all:
    # '&outFields=*' \
    # 'where=1%3D1' \
    URL_GREEN_RED = 'https://data.gov.il/api/3/action/datastore_search?' \
                    'resource_id=f1d13bbd-4f84-4cde-82ed-e075c942de12&limit=100000'  # 167
    SCHEDULER: BlockingScheduler = BlockingScheduler()
    SAVE_TO_HDFS: bool = False
    RUN_SCHEDULER: bool = True
    JSON_PATH = "graph_json.json"
    URL_GOV = 'https://data.gov.il/api/3/action/datastore_search?' \
              'resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000000'


def run_py_file(filename: str):
    process: Popen = subprocess \
        .Popen(["python3", f'{os.getcwd()}/Ruby_corona_charts/python/{filename}.py'], stdout=sys.stdout)  # search
        #.Popen([sys.executable, f'{os.getcwd()}\\{filename}.py'], stdout=sys.stdout, shell=True,
         #      creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)  # search
    # .Popen(["python", f'{os.getcwd()}\\start_search.py'], stdout=sys.stdout)  # search
    # TODO: print if there was a crush or Error
    process.communicate()  # wait for process to end


def firebase_config():
    config = {
        # "apiKey": "apiKey",
        # "authDomain": "projectId.firebaseapp.com",
        # "databaseURL": "https://databaseName.firebaseio.com",
        # "storageBucket": "projectId.appspot.com",
        # "serviceAccount": "path/to/serviceAccountCredentials.json"  # (optional)

        "apiKey": "AIzaSyCOt619fNqEuIgFpzf20h2cmC6tFeQYuTE",
        "authDomain": "corona-charts-33e8a.firebaseapp.com",
        "databaseURL": "https://corona-charts-33e8a-default-rtdb.firebaseio.com/",
        "storageBucket": "corona-charts-33e8a.appspot.com"
    }

    firebase = Firebase(config)

    Constants.db = firebase.database()
