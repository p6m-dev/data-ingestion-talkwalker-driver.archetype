import logging
import os
import requests
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import httpx



class OpensearchIndexer:

    def __init__(self, host, region, aws_key, aws_secret, index_name):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )

        self.logger = logging.getLogger()
        self.host = host
        self.region = region
        self.aws_key = aws_key  
        self.aws_secret = aws_secret
        self.index_name = index_name
        service = 'es'

        self.session = boto3.Session(
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=aws_secret,
            region_name=self.region,
        )

        self.credentials =self.session.get_credentials()
        awsauth = AWS4Auth(self.credentials.access_key, self.credentials.secret_key, region, service, session_token=self.credentials.token)

        self.search = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            http_compress=True,
            connection_class=RequestsHttpConnection
        )

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )



    async def ingest_to_opensearch(self, doc_id, content):
        try:
            document = {
                "doc_id": doc_id,
                "content": content
            }

            # Send the request.
            self.search.index(index=self.index, id=doc_id, body=document, refresh=True)

            print("Document indexed successfully with id: ", doc_id)

        except Exception as e:
            print(e)
            raise e
        return True

    