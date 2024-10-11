import logging
import requests
from . pinecone_index import Indexer


class PineconeIngestor:
    """Pine cone Ingestor Indexer"""

    def __init__(self, api_key, environment, index_name, embeddings):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )

        #TODO : Fix this from K8 config map
        self.doc_retriever_url = "http://34.220.33.50:8000"

        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.embeddings = embeddings

        self.indexer = Indexer(api_key, environment, index_name, embeddings)

        self.error_prefix = 'HTTP Error in HTTP request'
        self.logger = logging.getLogger()

    def get_text_documents_range(self):
        endpoint_url = f"{self.doc_retriever_url}/text-document/range/"
        self.logger.info(f'retrieving URL - {endpoint_url}')
        response = requests.get(endpoint_url)
        self.logger.info(f'response - {response}')
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'{self.error_prefix} - {response.status_code}')

    def get_text_document_metadata(self, doc_id):
        endpoint_url = f"{self.doc_retriever_url}/text-document/metadata/{doc_id}"
        self.logger.info(f'retrieving URL - {endpoint_url}')
        response = requests.get(endpoint_url)
        self.logger.info(f'response - {response}')
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'{self.error_prefix}- {response.status_code}')

    def get_text_document_content(self, doc_id):
        endpoint_url = f"{self.doc_retriever_url}/text-document/content/{doc_id}"
        self.logger.info(f'retrieving URL - {endpoint_url}')
        response = requests.get(endpoint_url)
        self.logger.info(f'response - {response}')
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f'{self.error_prefix}- {response.status_code}')

    def add_text(self, text_content):
        self.indexer.add_string(text_content)

