import logging
import json
import os
import time
from ..driver import Driver
from ingestors.bertopic.bertopic_ingestor import BertopicIngestor


class BertopicDriver(Driver):
    def __init__(self):
        Driver.__init__(self)
        self.input_bucket_name = None
        self.output_bucket_name = None
        self.text_bucket_name = None
        self.error_bucket_name = None

        self.object_storage = None

    def run(self):
        # start
        self.logger.setLevel(logging.INFO)
        self.logger.info("Input pipeline:")
        self.logger.info("============")

        # try:
        bertopic = BertopicIngestor()
        bertopic.train()

        # except (KeyboardInterrupt, TypeError, Exception) as e:
        #     self.logger.info(f"Task failed: {e}")