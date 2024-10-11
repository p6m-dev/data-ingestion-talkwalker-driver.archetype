import os, json, random, time
import boto3
import logging
import requests
from ingestors.ingestor import Ingestor


import pandas as pd
import datetime

from bertopic import BERTopic
from bertopic.representation import OpenAI
from bertopic.representation import PartOfSpeech
from bertopic.representation import KeyBERTInspired
from bertopic.vectorizers import ClassTfidfTransformer
from bertopic.representation import TextGeneration


# from config import Config
# Config manifest


class BertopicIngestor(Ingestor):
    def __init__(self):
        Ingestor.__init__(self)
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()
        # self.logger.setLevel(logging.ERROR)
        self.json_data_path = None
        self.docs = None

    def retrieve_data(self):
        """
        We need to call the get_document retriever here
        """
        self.json_data_path = "data/talk_walker_1694711122.jsonl"
        df = pd.read_json(self.json_data_path, lines=True)
        # df = pd.read_csv(file_path)
        docs = df["body"].to_list()
        self.docs = docs + docs
        self.logger.info(docs[0])

    def visualize_topics(self):
        topic_model = BERTopic(min_topic_size=30, verbose=True)
        topics, _ = topic_model.fit_transform(self.docs)
        freq = topic_model.get_topic_info()
        fig = topic_model.visualize_topics()
        fig.write_html("data/visualizations/topics.html")