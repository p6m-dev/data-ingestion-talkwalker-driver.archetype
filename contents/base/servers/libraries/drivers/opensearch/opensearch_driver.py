
import time
import logging
# from drivers.driver import Driver
from dotenv import load_dotenv

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from libraries.ingestors.opensearch.opensearch_ingestor import OpensearchIngestor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '...')))

class OpensearchDriver():
    def __init__(self):
        # Driver.__init__(self)

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )
        self.logger = logging.getLogger()

        load_dotenv()
        aws_region: str = os.getenv("AWS_REGION")
        aws_key: str = os.getenv("AWS_ACCESS_KEY")
        aws_secret: str = os.getenv("AWS_ACCESS_SECRET")
        opensearch_host: str = os.getenv("OPENSEARCH_HOST")
        opensearch_index_name: str = os.getenv("OPENSEARCH_INDEX_NAME")

        if not opensearch_host or not aws_region or not aws_key or not aws_secret or not opensearch_index_name:
            self.logger.error(
                "OPENSEARCH_HOST, AWS_REGION, AWS_ACCESS_KEY, AWS_ACCESS_SECRET or OPENSEARCH_INDEX_NAME exports are missing."
            )
            exit(1)

        self.ingestor = OpensearchIngestor(opensearch_host, aws_region, aws_key, aws_secret, opensearch_index_name)


    def run(self):
        self.logger.setLevel(logging.INFO)
        

        try:
            self.logger.info("============")
            self.logger.info("Input pipeline:")

            
            result = self.ingestor.get_text_documents_range()

            error_message_prefix = 'Error in Document Retriever API get_text_documents_range() method'

            if result is None:
                self.logger.error(f"{error_message_prefix}")
                return

            if 'status' not in result:
                self.logger.error(f"{error_message_prefix} - status was not found")
                return

            if not result['status'] and 'error_message' in result:
                self.logger.error(f"{error_message_prefix} - {result['error_message']}")
                return

            low_id = result['low_doc_id']
            high_id = result['high_doc_id']

            x = low_id
            while x <= high_id:

                print(x)
                print('======')


                try:
                    doc_result = self.ingestor.get_text_document_content(x)
                    self.ingestor.create(x, doc_result)
                except:
                    self.logger.exception("Error in HTTP request")

                x += 1

            self.logger.info("====  Complete ==========")
            time.sleep(600)

        except:
            self.logger.exception("Error encountered while processing a task.")



if __name__ == "__main__":
    driver = OpensearchDriver()
    driver.run()