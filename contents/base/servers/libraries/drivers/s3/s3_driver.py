import os
import sys
import time
import nltk
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libraries.converters import pdf2text
from libraries.converters import doc2text
from libraries.converters import pptx2text
from libraries.converters import html2text
from libraries.converters import list2text
from libraries.converters import speech2list
from libraries.ingestors.s3 import s3storage
from libraries.drivers.driver import Driver
from urllib.parse import unquote_plus
from vectordb.pinecone_index.index import Indexer
from langchain.embeddings import HuggingFaceEmbeddings
from vectordb.pinecone_index.pinecone import PineConeIndex

DOWNLOAD_PATH = "./data"
SPEECH_MODEL_SIZE = "medium"
TASK_TYPE = "s3-ingestor"
AGENT_NAME = f"s3-ingestor-{os.getpid()}"
KEY_PREFIX = "/Amazon S3/Buckets/p6m/private/s3/"


class S3Driver(Driver):
    """This class S3 driver consisting of business logic to ingest objects in a S3 bucket"""

    def __init__(self):
        """Constructor"""

        Driver.__init__(self)

        self.input_bucket_name = None
        self.output_bucket_name = None
        self.text_bucket_name = None
        self.error_bucket_name = None

        self.object_storage = None
        self.download_path = DOWNLOAD_PATH
        self.model_size = SPEECH_MODEL_SIZE
        self.key_prefix = KEY_PREFIX

    def initialize_buckets(self) -> None:
        """Validate all three S3 bucket parameters from environment variables"""

        input_bucket_name: str = os.getenv("INPUT_BUCKET_NAME")
        output_bucket_name: str = os.getenv("OUTPUT_BUCKET_NAME")
        text_bucket_name: str = os.getenv("TEXT_BUCKET_NAME")
        error_bucket_name: str = os.getenv("ERROR_BUCKET_NAME")

        if (
                not input_bucket_name
                or not output_bucket_name
                or not text_bucket_name
                or not error_bucket_name
        ):
            self.logger.error(
                """INPUT_BUCKET_NAME, OUTPUT_BUCKET_NAME"""
                """ TEXT_BUCKET_NAME or ERROR_BUCKET_NAME exports are missing."""
            )
            exit(1)

        self.input_bucket_name = input_bucket_name
        self.output_bucket_name = output_bucket_name
        self.text_bucket_name = text_bucket_name
        self.error_bucket_name = error_bucket_name

    def authenticate_s3(self):
        """Authenticate S3 credentials"""

        obj_storage = s3storage.S3Storage()
        if not obj_storage.authenticate():
            self.logger.error(
                "AWS Credentials AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION environment exports are missing"
            )
            exit(1)

        self.object_storage = obj_storage

    def initialize_pinecone(self):
        api_key: str = os.getenv("PINECONE_API_KEY")
        environment: str = os.getenv("PINECONE_ENVIRONMENT")
        index_name: str = os.getenv("PINECONE_INDEX_NAME")

        if not api_key or not environment or not index_name:
            self.logger.error(
                "PINECONE_API_KEY, PINECONE_ENVIRONMENT or PINECONE_INDEX_NAME exports are missing."
            )
            exit(1)

        pine = PineConeIndex(index_name, HuggingFaceEmbeddings(), api_key, environment)
        index = Indexer(pine)
        # initialize
        nltk.download("punkt")

    def download_object(self, key_name: str, file_path: str) -> bool:
        """This method copies objects from input bucket to a local directory"""

        self.logger.info(f"downloading object with key {key_name} to {file_path}")

        if not self.object_storage.download_file(
                self.input_bucket_name, key_name, file_path
        ):
            self.logger.error(
                f"Object with key {key_name} could not be downloaded to {file_path}"
            )
            return False
        else:
            self.logger.info(
                f"Object with key {key_name} was downloaded to {file_path}"
            )
            return True

    def upload_object(self, file_path, key_name: str) -> bool:
        """This method copies file from a local directory to the text bucket"""

        self.logger.info(f"file path = {file_path}")
        self.logger.info(f"key name = {key_name}")

        self.logger.info(
            f"uploading file {file_path} to bucket {self.text_bucket_name} with key {key_name}"
        )
        if not self.object_storage.upload_file(
                file_path, self.text_bucket_name, key_name
        ):
            self.logger.error(
                f"File {file_path} copy to bucket {self.text_bucket_name} with key {key_name} failed."
            )
            return False
        else:
            self.logger.info(
                f"File {file_path} was copied to bucket {self.text_bucket_name}."
            )
            return True

    def move_upon_success(self, key_name: str) -> bool:
        """move_upon_success() method moves objects from an input bucket to an output bucket after succesful convrsion"""

        self.logger.info(
            f"Moving object {key_name} from bucket {self.input_bucket_name} to bucket {self.output_bucket_name}."
        )
        if not self.object_storage.move_file(
                self.input_bucket_name, key_name, self.output_bucket_name
        ):
            self.logger.error(
                f"Object move from bucket {self.input_bucket_name} to bucket {self.output_bucket_name} with key {key_name} failed."
            )
            return False
        else:
            self.logger.info(
                f"Object {key_name} from bucket {self.input_bucket_name} to bucket {self.output_bucket_name} was moved."
            )
            return True

    def move_upon_failure(self, key_name: str) -> bool:
        """move_upon_failure() method moves objects from an input bucket to an error bucket after failure to convert"""

        self.logger.info(
            f"Moving object {key_name} from bucket {self.input_bucket_name} to bucket {self.error_bucket_name}."
        )
        if not self.object_storage.move_file(
                self.input_bucket_name, key_name, self.error_bucket_name
        ):
            self.logger.error(
                f"Object move from bucket {self.input_bucket_name} to bucket {self.error_bucket_name} with key {key_name} failed."
            )
            return False
        else:
            self.logger.info(
                f"Object {key_name} from bucket {self.input_bucket_name} to bucket {self.error_bucket_name} was moved."
            )
            return True

    def extract_text(self, key_name: str, file_path: str, text_key_name: str) -> bool:
        """Converts file format of one file"""

        success = False

        self.logger.info(f"extracting text:{key_name}")

        split_tup = os.path.splitext(file_path)
        file_extension = split_tup[1].lower()
        text_file_path = split_tup[0] + ".txt"

        if file_extension == ".pdf":
            pdf_converter = pdf2text.PDF2Text()
            pdf_converter.configure(file_path, text_file_path)
            success = pdf_converter.convert()

        elif file_extension == ".doc" or file_extension == ".docx":
            doc_converter = doc2text.Doc2Text()
            doc_converter.configure(file_path, text_file_path)
            success = doc_converter.convert()

        elif file_extension == ".ppt" or file_extension == ".pptx":
            ppt_converter = pptx2text.Pptx2Text()
            ppt_converter.configure(file_path, text_file_path)
            success = ppt_converter.convert()

        elif file_extension == ".htm" or file_extension == ".html":
            html_converter = html2text.Html2Text()
            html_converter.configure(file_path, text_file_path)
            success = html_converter.convert()

        elif (
                file_extension == ".wav"
                or file_extension == ".mp3"
                or file_extension == ".mp4"
                or file_extension == ".mpeg"
                or file_extension == ".mpga"
                or file_extension == ".m4a"
                or file_extension == ".webm"
        ):
            transcriber = speech2list.Speech2List()
            transcriber.configure(file_path, self.model_size)
            lines = transcriber.convert()
            if lines is None:
                success = False
            else:
                sink = list2text.List2Text()
                sink.configure(text_file_path)
                success = sink.convert(lines)

        if not success:
            self.logger.error(f"Failed to convert file {file_path}")

        if success:
            if os.path.isfile(text_file_path):
                self.upload_object(text_file_path, text_key_name)
            self.move_upon_success(key_name)
        else:
            self.move_upon_failure(key_name)

        # cleanup
        if os.path.isfile(file_path):
            os.remove(file_path)
        if os.path.isfile(text_file_path):
            os.remove(text_file_path)

        return True

    def process_object(self, task_id: str, original_key: str, file_path: str) -> bool:
        """download an object from object store and process file for format conversion"""

        status = False
        self.logger.info(f"processing key :{original_key}")

        try:
            if self.download_object(original_key, file_path):
                text_key_name = original_key + ".txt"
                status = self.extract_text(original_key, file_path, text_key_name)

                original_document_metadata = self.put_original_document_metadata(
                    task_queue_id=task_id,
                    task_type=TASK_TYPE,
                    task_agent=AGENT_NAME,
                    original_document_uri=original_key,
                    metadata={},
                )
                self.logger.info(
                    f"original_document_metadata was uploaded to metadata api : {original_document_metadata}"
                )

                put_text_document_metadata = self.put_text_document_metadata(
                    task_queue_id=task_id,
                    task_type=TASK_TYPE,
                    task_agent=AGENT_NAME,
                    original_document_uri=original_key,
                    text_document_uri=text_key_name,
                    file_metadata={},
                    page_metadata={},
                )
                self.logger.info(f"text_document_metadata was uploaded to metadata api : {put_text_document_metadata}")
        except:
            self.logger.exception("File format conversion failed")

        return status

    def run(self):
        """Main worker function"""

        os.makedirs(self.download_path, exist_ok=True)

        self.initialize_buckets()
        self.authenticate_s3()
        self.initialize_pinecone()

        self.logger.setLevel(logging.INFO)

        while True:
            # get ready for next cycle
            time.sleep(5)

            try:
                # todo: agent name

                task = self.claim_task(TASK_TYPE, AGENT_NAME)
                print(task)
                if task is None:
                    self.logger.error(f"Error in Task API claim() method")
                    continue

                if 'status' not in task:
                    self.logger.error(f"Error in Task API claim() method - key [status] was not found")
                    continue

                if not task['status'] and 'error_message' in task and \
                        task['error_message'] != 'No unclaimed task found':
                    self.logger.error(f"Error in Task API claim() method - {task['error_message']}")
                    continue

                if not task['status'] and 'error_message' in task and \
                        task['error_message'] == 'No unclaimed task found':
                    self.logger.info(f"Waiting for work - {task['error_message']}")
                    continue

                task_id = task["id"]
                task_query = task["query"]
                self.logger.info(f"Query: {task_query}")

                self.logger.info("============")
                self.logger.info(f"task id {task_id} started.")
                self.logger.info("Input pipeline:")

                objects = self.object_storage.list_objects(self.input_bucket_name)

                # process every object in the bucket
                for original_key in objects:
                    """
                    reference https://docs.aws.amazon.com/lambda/latest/dg/with-s3-tutorial.html#with-s3-tutorial-test-image
                    """

                    unquoted_key = unquote_plus(original_key)
                    modified_key = original_key.replace("/", "")
                    file_path = self.download_path + "/" + modified_key

                    # source_key = f"{self.key_prefix}/{task_id}/key"

                    self.process_object(task_id, original_key, file_path)

                self.logger.info("============")
                self.logger.info(f"Task id {task_id} completed.")

                self.task_completed(
                    task_id, True, f"Task id {task_id} completed successfully."
                )

            except:
                self.logger.exception("Error encountered while processing a task.")