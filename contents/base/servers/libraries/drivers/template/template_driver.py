import logging
import json
import os, sys
import time
from datetime import datetime
from pprint import pprint

from libraries.drivers.driver import Driver
from libraries.drivers.talkwalker.credits import get_credits_estimation
from libraries.ingestors.s3 import s3storage
from libraries.ingestors.template.template_ingestor import Template

TASK_TYPE = "talkwalker"


class TemplateDriver(Driver):
    def __init__(self):
        Driver.__init__(self)
        self.input_bucket_name = None
        self.output_bucket_name = None
        self.text_bucket_name = None
        self.error_bucket_name = None
        self.object_storage = None
        self.talk_walker = None

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

    def save_data_to_file(self, data, jsonl_filename):
        with open(jsonl_filename, "a") as f:
            for item in data:
                # print(item)
                f.write(json.dumps(item) + "\n")

    def run(self):
        # start
        self.logger.setLevel(logging.INFO)
        self.logger.info("Input pipeline:")
        self.logger.info("============")

        task = self.claim_task(TASK_TYPE, 10)

        if task["status"] == False:
            self.logger.info(f"{task['error_message']}")
            return

        task_id = task["id"]
        query = json.loads(task["query"].replace("'", '"'))

        self.initialize_buckets()
        self.logger.info(f"file path = {self.input_bucket_name}")
        self.authenticate_s3()
        self.logger.info(f"object_storage = {self.object_storage}")

        try:
            self.talk_walker = Template(query)

            timestamp = int(time.time())  # Generate a unique timestamp
            jsonl_filename = (
                f"template_{timestamp}.jsonl"  # Include timestamp in the filename
            )
            error_filename = (
                f"template_{timestamp}.errors.txt"  # Include timestamp in the filename
            )
            jsonl_file_path = os.path.join("libraries/data", jsonl_filename)
            error_file_path = os.path.join("libraries/data", error_filename)
            self.logger.info(jsonl_file_path)

            for data in self.talk_walker.retrieve_data():
                self.save_data_to_file([data], jsonl_file_path)

            # object_storage_key_for_results
            s3_jsonl_key_name = (
                f"p6m/public/raw/{TASK_TYPE}/{timestamp}/{task_id}.jsonl"
            )

            # self.upload_file(
            #     jsonl_file_path, self.output_bucket_name, s3_jsonl_key_name
            # )
            # original_document_metadata = self.put_original_document_metadata(
            #     task_queue_id=task_id,
            #     task_type=TASK_TYPE,
            #     task_agent=task.get("claimed_by_agent", "10"),
            #     original_document_uri=f"s3://legos-docs/{s3_jsonl_key_name}",
            #     metadata={},
            # )
            # self.logger.info(
            #     f"original_document_metadata: {original_document_metadata}"
            # )

            # jsonl_filename_txt = f"{jsonl_filename}.txt"
            # self.extract_text(jsonl_filename, jsonl_file_path, jsonl_filename_txt)
            # put_text_document_metadata = self.put_text_document_metadata(
            #     task_queue_id=task_id,
            #     task_type=TASK_TYPE,
            #     task_agent=task.get("claimed_by_agent", "10"),
            #     original_document_uri=f"s3://legos-docs/{s3_jsonl_key_name}",
            #     text_document_uri=jsonl_filename_txt,
            #     file_metadata={},
            #     page_metadata={},
            # )
            # self.logger.info(f"text_document_metadata: {put_text_document_metadata}")

            task_completed = self.task_completed(
                object_storage_key_for_results=f"s3://legos-docs/{s3_jsonl_key_name}",
                task_id=task_id,
                success=True,
                message="Task completed",
            )
            if "message" in task_completed:
                self.logger.info(task_completed["message"])
            self.logger.info("Completed.")
            self.logger.info(f"\n=========================================\n")
            self.logger.info(f"\n===============Completed=================\n")

        except (KeyboardInterrupt, TypeError, Exception) as e:
            self.task_completed(
                object_storage_key_for_results=None,
                task_id=task_id,
                success=False,
                message="Task not completed",
            )
            self.logger.info(f"Task failed: {e}")