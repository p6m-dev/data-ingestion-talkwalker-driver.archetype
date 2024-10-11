import logging
import requests
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from libraries.ingestors.opensearch.opensearch_indexer import OpensearchIndexer


class OpensearchIngestor:
    """Open search Ingestor Indexer"""

    def __init__(self, host, region, aws_key, aws_secret, index_name):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )

        #TODO : Fix this from K8 config map
        self.doc_retriever_url = "http://34.220.33.50:8000"

        self.host = host
        self.region = region
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.index_name = index_name

        
        self.indexer = OpensearchIndexer(host, region, aws_key, aws_secret, index_name)
        self.error_prefix = 'HTTP Error in HTTP request'
        self.logger = logging.getLogger()

    def get_text_documents_range(self):
        endpoint_url = f"{self.doc_retriever_url}/text-document/range/"
        print(endpoint_url)
        response = requests.get(endpoint_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'{self.error_prefix} - {response.status_code}')

    def get_text_document_metadata(self, doc_id):
        endpoint_url = f"{self.doc_retriever_url}/text-document/metadata/{doc_id}"
        response = requests.get(endpoint_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'{self.error_prefix}- {response.status_code}')

    def get_text_document_content(self, doc_id):
        endpoint_url = f"{self.doc_retriever_url}/text-document/content/{doc_id}"
        response = requests.get(endpoint_url)
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f'{self.error_prefix}- {response.status_code}')

    def create(self,doc_id, text_content):
        self.indexer.ingest_to_opensearch(doc_id, text_content)

