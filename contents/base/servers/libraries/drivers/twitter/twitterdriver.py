import logging
import json
import os
import time

from libraries.drivers.driver import Driver


from libraries.ingestors.twitter.twitter_ingestor import Twitter
from libraries.ingestors.s3 import s3storage
from libraries.converters import jsonl2text


class TwitterDriver(Driver):
    def __init__(self):
        Driver.__init__(self)
        self.input_bucket_name = None
        self.output_bucket_name = None
        self.text_bucket_name = None
        self.error_bucket_name = None

        self.object_storage = None

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

    def upload_file(self, file_path, bucket_name, key_name: str) -> bool:
        """This method copies file from a local directory to the text bucket"""

        self.logger.info(f"file path = {file_path}")

        self.logger.info(f"uploading file {file_path} to bucket {bucket_name}")
        if not self.object_storage.upload_file(file_path, bucket_name, key_name):
            self.logger.error(f"File {file_path} copy to bucket {bucket_name} failed.")
            return False
        else:
            self.logger.info(f"File {file_path} was copied to bucket {bucket_name}.")
            return True

    def extract_text(self, key_name: str, file_path: str, jsonl_filename_txt) -> bool:
        """Converts file format of one file"""

        success = False

        self.logger.info(f"extracting text:{key_name}")

        split_tup = os.path.splitext(file_path)
        file_extension = split_tup[1].lower()
        text_file_path = split_tup[0] + ".txt"

        if file_extension == ".jsonl":
            jsonl_converter = jsonl2text.JSONL2Text()
            jsonl_converter.configure(file_path, text_file_path)
            success = jsonl_converter.convert()

        if not success:
            self.logger.error(f"Failed to convert file {file_path}")

        if success:
            if os.path.isfile(text_file_path):
                self.upload_file(
                    text_file_path, self.text_bucket_name, jsonl_filename_txt
                )
        else:
            # self.move_upon_failure(key_name)
            # todo:
            pass

        # cleanup
        if os.path.isfile(file_path):
            os.remove(file_path)
        if os.path.isfile(text_file_path):
            os.remove(text_file_path)

        return True

    def _save_tweets_to_json(self, tweets, jsonl_filename):
        with open(jsonl_filename, "a") as f:
            for tweet in tweets:
                f.write(json.dumps(tweet) + "\n")

    def run(self):
        # start
        self.logger.setLevel(logging.INFO)
        self.logger.info("Input pipeline:")
        self.logger.info("============")

        task = self.claim_task("twitter", 10)
        if task["status"] == False:
            self.logger.info(f"{task['error_message']}")
            return

        task_id = task["id"]
        task_query = task["query"]
        self.logger.info(f"Query: {task_query}")

        self.initialize_buckets()
        self.logger.info(f"file path = {self.input_bucket_name}")
        self.authenticate_s3()
        self.logger.info(f"object_storage = {self.object_storage}")

        try:
            twitter = Twitter()
            timestamp = int(time.time())  # Generate a unique timestamp
            jsonl_filename = (
                f"tweets_{timestamp}.jsonl"  # Include timestamp in the filename
            )
            jsonl_file_path = f"data/{jsonl_filename}"

            tweet_items = []  # write tweets to json in a back of 10
            tweet_number = 0
            for tweet_data in twitter.get_tweets_by_query(task_query):
                tweet_number += 1
                tweet_items.append(tweet_data)
                if len(tweet_items) == 10:
                    self._save_tweets_to_json(tweet_items, jsonl_file_path)
                    tweet_items = []  # clear the list for the next batch
                self.logger.info(f"Tweet number: {tweet_number}")

            # save the remaining less than 10 tweets to file
            if tweet_items:
                self._save_tweets_to_json(tweet_items, jsonl_file_path)

            task_completed = self.task_completed(
                task_id=task_id, success=True, message="Task completed"
            )
            if "message" in task_completed:
                self.logger.info(task_completed["message"])
            self.logger.info("Completed.")
            self.upload_file(jsonl_file_path, self.output_bucket_name, jsonl_filename)
            original_document_metadata = self.put_original_document_metadata(
                task_queue_id=task_id,
                task_type="twitter",
                task_agent="10",
                original_document_uri=jsonl_filename,
                metadata={},
            )
            self.logger.info(
                f"original_document_metadata: {original_document_metadata}"
            )

            jsonl_filename_txt = f"{jsonl_filename}.txt"
            self.extract_text(jsonl_filename, jsonl_file_path, jsonl_filename_txt)
            put_text_document_metadata = self.put_text_document_metadata(
                task_queue_id=task_id,
                task_type="twitter",
                task_agent="10",
                original_document_uri=jsonl_filename,
                text_document_uri=jsonl_filename_txt,
                file_metadata={},
                page_metadata={},
            )
            self.logger.info(f"text_document_metadata: {put_text_document_metadata}")

        except (KeyboardInterrupt, TypeError, Exception) as e:
            self.task_completed(
                task_id=task_id, success=False, message="Task not completed"
            )
            self.logger.info(f"Task failed: {e}")