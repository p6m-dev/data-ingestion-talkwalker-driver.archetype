import os, json, random, time
import boto3
import logging
import requests
from pprint import pprint
from fake_useragent import UserAgent
from datetime import date, timedelta
from datetime import datetime
from types import SimpleNamespace
from urllib.parse import urlparse


from libraries.ingestors.ingestor import Ingestor


class Template(Ingestor):
    def __init__(self, query=dict):
        Ingestor.__init__(self)

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()
        # self.logger.setLevel(logging.ERROR)

    def retrieve_data(self):
        # Loop through each day from the start_date to the end_date
        for n in range(1000):
            # yield self.search_results(url)
            yield {"id": n}