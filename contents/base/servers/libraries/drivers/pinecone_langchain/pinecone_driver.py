import os
import time
import nltk
import logging
from drivers.driver import Driver
from vectordb.pinecone_index.index import Indexer
from langchain.embeddings import HuggingFaceEmbeddings
from vectordb.pinecone_index.pinecone import PineConeIndex
from ingestors.pinecone_langchain.pinecone_ingestor import PineconeIngestor

TASK_TYPE = "pinecone-langchain-ingestor"
AGENT_NAME = f"pinecone-langchain-{os.getpid()}"


class PineconeDriver(Driver):
    """This class S3 driver consisting of business logic to ingest objects in a S3 bucket"""

    def __init__(self):
        """Constructor"""

        Driver.__init__(self)

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )
        self.logger = logging.getLogger()

        api_key: str = os.getenv("PINECONE_API_KEY")
        environment: str = os.getenv("PINECONE_ENVIRONMENT")
        index_name: str = os.getenv("PINECONE_INDEX_NAME")

        if not api_key or not environment or not index_name:
            self.logger.error(
                "PINECONE_API_KEY, PINECONE_ENVIRONMENT or PINECONE_INDEX_NAME exports are missing."
            )
            exit(1)

        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.embeddings = HuggingFaceEmbeddings()

        pine = PineConeIndex(index_name, self.embeddings, api_key, environment)
        index = Indexer(pine)

        # initialize
        nltk.download("punkt")

    def run(self):
        """Main worker function"""

        self.logger.setLevel(logging.INFO)

        while True:
            # get ready for next cycle
            time.sleep(5)

            try:

                self.logger.info("============")
                self.logger.info("Input pipeline:")

                # main logic
                ingestor = PineconeIngestor(self.api_key, self.environment, self.index_name, self.embeddings)
                result = ingestor.get_text_documents_range()

                error_message_prefix = 'Error in Document Retriever API get_text_documents_range() method'

                if result is None:
                    self.logger.error(f"{error_message_prefix}")
                    continue

                if 'status' not in result:
                    self.logger.error(f"{error_message_prefix} - status was not found")
                    continue

                if not result['status'] and 'error_message' in result:
                    self.logger.error(f"{error_message_prefix} - {result['error_message']}")
                    continue

                low_id = result['low_doc_id']
                high_id = result['high_doc_id']

                # print(result)
                # print('======')
                # print(low_id)
                # print('======')
                # print(high_id)
                # print('======')

                x = low_id
                while x <= high_id:

                    print(x)
                    print('======')

                    # if x == 2 or x == 4 or x ==15:
                    #     x += 1
                    #     continue

                    try:
                        doc_result = ingestor.get_text_document_content(x)
                        #print(doc_result)
                        ingestor.add_text(doc_result)
                    except:
                        self.logger.exception("Error in HTTP request")

                    x += 1

                self.logger.info("====  Complete ==========")
                time.sleep(600)

            except:
                self.logger.exception("Error encountered while processing a task.")