import logging
import json
import os, sys
import time
import traceback
from datetime import datetime
from libraries.converters import jsonl2text
from libraries.drivers.driver import Driver
from libraries.ingestors.s3 import s3storage
from libraries.drivers.talkwalker.credits import (
    get_credits_estimation,
    is_valid_project_id,
)
from libraries.logs.constants import LogMetricsConstants
from libraries.logs.cloudlogs import CloudMultiLogMetrics
from libraries.ingestors.twitter.twitter_ingestor import Twitter
from libraries.ingestors.talkwalker.talkwalker_ingestor import TalkWalker


class Constants:
    TWITTER_IDS_COUNT = 100
    DRIVER_NAME = "talkwalker"  # must match postgres database lookup table
    APPLICATION_NAME = DRIVER_NAME
    VERSION = 17

class TalkWalkerDriver(Driver):
    def __init__(self):
        Driver.__init__(self)

        self.logger = logging.getLogger()
        self.root_logger = logging.getLogger()
        self.object_storage = None
        self.talk_walker = None
        self.buckets = None
        self.application_name = f'({Constants.APPLICATION_NAME} v.{Constants.VERSION} k8s/airflow) - '
        print(f'{self.application_name} initialized.')

    def initialize_buckets(self) -> None:
        """Validate all three S3 bucket parameters from environment variables"""

        input_bucket_name: str = os.getenv("INPUT_BUCKET_NAME")
        output_bucket_name: str = os.getenv("OUTPUT_BUCKET_NAME")
        text_bucket_name: str = os.getenv("TEXT_BUCKET_NAME")
        error_bucket_name: str = os.getenv("ERROR_BUCKET_NAME")
        logs_bucket_name: str = os.getenv("LOGS_BUCKET_NAME")

        if (
                not input_bucket_name
                or not output_bucket_name
                or not text_bucket_name
                or not error_bucket_name
                or not logs_bucket_name
        ):
            self.root_logger.error(
                """INPUT_BUCKET_NAME, OUTPUT_BUCKET_NAME"""
                """ TEXT_BUCKET_NAME or ERROR_BUCKET_NAME or LOGS_BUCKET_NAME exports are missing."""
            )
            exit(1)

        self.buckets = {
            'input': input_bucket_name,
            'output': output_bucket_name,
            'text': text_bucket_name,
            'logs': logs_bucket_name,
            'errors': error_bucket_name,
        }

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

        self.logger.info(f"{self.application_name} - Uploading file {file_path} to bucket {bucket_name}")
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
                    text_file_path, self.buckets['text'], jsonl_filename_txt
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

    def transform_tweet_data(self, tweet_data, item):
        """Method to transform the talkwalker item, tweet data and return it as dict"""
        created_at = tweet_data["created_at"].replace(" ", "").replace("\n", "")
        dt_obj = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        created_epoch_unix = int(dt_obj.timestamp())

        data = {**item, **tweet_data}
        # NOTE: only replace the published date with twitter date if the talkwalker date
        # is not available or is invalid
        if data["published"] == -1 or data["published"] == 0:
            data["published"] = created_epoch_unix
            data["x-p6m-publish-source"] = "twitter"
        text = data.pop("text")
        data["body"] = text
        data["word_count"] = len(text.split())
        data["twitter_data"] = tweet_data
        data["url"] = tweet_data["id"]
        # print(f'tweet text inside merge =  {data["body"]}')
        return data

    def save_data_to_file(self, data, jsonl_filename):
        with open(jsonl_filename, "a") as f:
            for item in data:
                # print(item)
                f.write(json.dumps(item) + "\n")

    @staticmethod
    def get_item_by_id(items, external_id):
        for item in items:
            if item["external_id"] == external_id:
                return item
        return ""

    def merge_tweet_data(self, items, error_file_path):
        twitter = Twitter()
        tweets_data = twitter.get_tweets_by_ids(
            [item["external_id"] for item in items], error_file_path
        )
        # self.logger.info(
        #     f"Twitter Lookup:  valid= {len(tweets_data['data']) }, invalid={len(tweets_data['errors']) } from original TW count of {len(items)}"
        # )

        data = []
        if len(tweets_data["errors"]):
            self.logger.info(
                f"NOT FOUND BEFORE - second try: {[error['value'] for error in tweets_data['errors']]}"
            )
            time.sleep(15)
            tweets_second = twitter.get_tweets_by_ids(
                [error["value"] for error in tweets_data["errors"]], error_file_path
            )

            tweets_data["data"] = tweets_data["data"] + tweets_second["data"]
            tweets_data["errors"] = tweets_second["errors"]

            self.logger.info(
                f"NOT FOUND AFTER - second try: {[error['value'] for error in tweets_data['errors']]}"
            )
            # self.talk_walker.log_error(f"Twitter errors {tweets_data['errors']}")

            if len(tweets_data["errors"]):
                self.logger.info(
                    f"NOT FOUND BEFORE - third try: {[error['value'] for error in tweets_data['errors']]}"
                )
                time.sleep(15)
                tweets_third = twitter.get_tweets_by_ids(
                    [error["value"] for error in tweets_data["errors"]], error_file_path
                )

                tweets_data["data"] = tweets_data["data"] + tweets_third["data"]
                tweets_data["errors"] = tweets_third["errors"]

                self.talk_walker.twitter_errors = self.talk_walker.twitter_errors + len(
                    tweets_data["errors"]
                )
                self.logger.info(
                    f"NOT FOUND AFTER - third try: {[error['value'] for error in tweets_data['errors']]}"
                )
                # self.talk_walker.log_error(f"Twitter errors {tweets_data['errors']}")
            self.talk_walker.twitter_errors = self.talk_walker.twitter_errors + len(
                tweets_data["errors"]
            )

        # TODO - the loop below should iterate on TW items
        # instead of tweet items as not all twitter hydration will succeed.
        # this way all un-hydrated twitter items from TW will be present in the output,
        # at lease (even if un-hydrated).

        for tweet in tweets_data["data"]:
            try:
                original_tw_item = self.get_item_by_id(items, tweet["id"])
                merged_items = self.transform_tweet_data(tweet, original_tw_item)
                data.append(merged_items)

            except Exception as e:
                self.logger.error(f"Exception in twitter TW merge!")
                self.logger.exception(e)

        for tweet in tweets_data["errors"]:
            try:
                original_tw_item = self.get_item_by_id(items, tweet["value"])
                original_tw_item["twitter_error"] = tweet
                original_tw_item.pop("x-p6m-publish-source", None)
                data.append(original_tw_item)

            except Exception as e:
                self.logger.error(f"Exception in twitter Errors and TW merge!")
                self.logger.exception(e)

        self.logger.info(
            f"Tweets merged. TW = {len(items)}. valid = {len(tweets_data['data'])}.  invalid = {len(tweets_data['errors'])} Merged = {len(data)}"
        )
        return data

    def setup_job_logger(self, root_logger: logging.Logger, project_id: str, topic_id: str, time_stamp: str):

        # one needs to have a good understanding of AWS Cloudwatch metrics dimensions

        dimensions = {
            'name_1': 'topic_id',
            'value_1': topic_id,
            'name_2': 'project_id',
            'value_2': project_id,
            'name_3': 'timestamp',
            'value_3': time_stamp
        }

        self.logger = CloudMultiLogMetrics(
            namespace=Constants.APPLICATION_NAME,  # usually application name
            buckets=self.buckets,
            dimensions=dimensions
        )

    def terminate_job_logger(self):
        self.logger.finalize()
        self.logger = None

    def run(self, params):
        # start
        self.root_logger.setLevel(logging.INFO)
        # task = self.claim_task(Constants.DRIVER_NAME, 10)
        #
        # if task["status"] == False:
        #     self.root_logger.info(f"{task['error_message']}")
        #     return

        agent_name = f"{self.application_name}"

        self.root_logger.info(f'Running application = {agent_name}')
        self.root_logger.info(f'with parameters {params}')

        self.root_logger.info(f"{self.application_name} Setting up access to s3 buckets.")
        self.initialize_buckets()
        self.root_logger.info(f"{self.application_name} Access to s3 buckets complete.")

        self.root_logger.info(f"Output Bucket = {self.buckets['output']}")
        self.root_logger.info(f"Log Bucket = {self.buckets['logs']}")
        self.root_logger.info(f"Error Bucket = {self.buckets['errors']}")

        # NOTE  sample payload
        """

         {
            "project_id": "ad6bc12c-bb4e-4cbd-9d27-3250d40d6305",
            "topic_id": "lp1tech7_gq0y2dnq4fgv",
            "get_news_links": false,
            "from_date": "2023-11-16",
            "to_date": "2023-11-15"
         }

        project_id: if the project_id is not provided, the PROJECT_ID form env is used.
        from_date: if from_date is not specified, TODAY's date will be used by default.
        to_date: if to_date is not specified, it defaults to 30 days back from from_date.
        """
        # query = json.loads(task["query"].replace("'", '"'))
        # task_id = task["id"]
        # topic_id = (query.get("topic_id", "")).strip()
        # project_id = (query.get("project_id", "")).strip()

        project_id = params['project_id']
        topic_id = params['topic_id']
        task_id = params['task_id']
        from_date = params['from_date']
        to_date = params['to_date']

        if topic_id == "":
            self.root_logger.info(f"missing topic_id - {topic_id}")
            return

        # Note: If the project_id is not specified, the PROJECT_ID from env will be used.
        if project_id == "":
            project_id = os.getenv("PROJECT_ID")

        # we have a topic id to run now, so initialize job specific logger

        timestamp = int(time.time())  # Generate a unique timestamp

        self.setup_job_logger(self.root_logger, project_id, topic_id, str(timestamp))

        # self.logger is available now as a per job logger
        self.logger.info(f"{self.application_name} New task id = {task_id} running at timestamp = {timestamp} topic id = {topic_id}")

        self.authenticate_s3()
        try:
            self.talk_walker = TalkWalker(params, self.logger)

            # # Note: If the project_id is not specified, the PROJECT_ID from env will be used.
            # if project_id != "":
            #     self.talk_walker.project_id = project_id
            # else:
            #     self.talk_walker.project_id = os.getenv("PROJECT_ID")
            #
            # """
            # check if the Project ID is valid
            # """
            # if not is_valid_project_id(
            #         self.talk_walker.access_token, self.talk_walker.project_id
            # ):
            #     self.logger.info(f"Invalid project_id - {self.talk_walker.project_id}")
            #     self.task_completed(
            #         task_id=task_id,
            #         success=False,
            #         message=f"Invalid project_id - {self.talk_walker.project_id}",
            #         object_storage_key_for_results=None,
            #     )
            #     self.terminate_job_logger()
            #     return

            """
            check if the topic is valid and that we have enough credits
            """
            can_exec_topic = get_credits_estimation(
                self.talk_walker.access_token, topic_id, self.talk_walker.project_id
            )
            print(can_exec_topic)
            available_credits = can_exec_topic["available_credits"]
            self.talk_walker.required_credits = can_exec_topic["required_credits"]

            if self.talk_walker.required_credits == -1:
                logging.error(f"invalid topic id: {topic_id}")
                # self.task_completed(
                #     task_id=task_id,
                #     success=False,
                #     message=f"invalid topic id: {topic_id}",
                #     object_storage_key_for_results=None,
                # )
                self.terminate_job_logger()
                exit(1)

            if can_exec_topic["enough_credits_available"] == False:
                logging.error(f"No enough credits available for: {topic_id}")
                logging.error(
                    f"Available credits: {available_credits}, required credits: {self.talk_walker.required_credits}"
                )
                # self.task_completed(
                #     task_id=task_id,
                #     success=False,
                #     message=f"No enough credits available for: {topic_id}. Available credits: {available_credits}, required credits: {self.talk_walker.required_credits}",
                #     object_storage_key_for_results=None,
                # )
                self.terminate_job_logger()
                exit(1)

            self.logger.info(
                f"{self.application_name} Topic: {topic_id},  total items to be retrieved: {self.talk_walker.required_credits}"
            )

            jsonl_filename = f"{Constants.APPLICATION_NAME}_{topic_id}_{timestamp}.jsonl"  # Include timestamp in the filename
            error_filename = f"{Constants.APPLICATION_NAME}_{topic_id}_{timestamp}.errors.txt"  # Include timestamp in the filename

            path = './data'

            # check whether directory already exists
            if not os.path.exists(path):
              os.mkdir(path)
              self.logger.info(f"{self.application_name} Folder {path} created!")
            else:
              self.logger.info(f"{self.application_name} Folder {path} already exists")

            jsonl_file_path = os.path.join(path, jsonl_filename)
            error_file_path = os.path.join(path, error_filename)

            self.logger.info(f'local json file path = {jsonl_file_path}')
            self.logger.info(f'local error file path = {error_file_path}')

            tweet_items = []  # list to hold tweet items for batching

            job_status_update = {
                "total_retrieved": 0,
                "total_twitter": 0,
                "twitter_errors": 0,
                "self.talk_walker.total_saved": 0,
                "latest_errors": [],
            }

            loop_count = 0

            for data in self.talk_walker.retrieve_data():
                for item in data:

                    loop_count += 1

                    if item["external_provider"] == "twitter":
                        tweet_items.append(item)  # add the item to the batch list

                        # If we've reached 100 items, get the tweets and write to the file
                        if len(tweet_items) == Constants.TWITTER_IDS_COUNT:
                            self.logger.info(
                                f"Batched tweeter items = {len(tweet_items)} "
                            )
                            merged_items = self.merge_tweet_data(
                                tweet_items, error_file_path
                            )
                            self.logger.info(
                                f"Merged tweeter items = {len(merged_items)} from original TW items  = {len(tweet_items)}"
                            )
                            self.talk_walker.total_saved += len(merged_items)
                            self.save_data_to_file(merged_items, jsonl_file_path)
                            tweet_items = []
                    else:
                        self.talk_walker.total_saved += 1
                        self.save_data_to_file([item], jsonl_file_path)

                    job_status_update = {
                        "total_retrieved": self.talk_walker.total_item_count,
                        "total_twitter": self.talk_walker.total_twitter_count,
                        "twitter_errors": self.talk_walker.twitter_errors,
                        "total_saved": self.talk_walker.total_saved,
                        "latest_errors": self.talk_walker.get_latest_errors(),
                    }

                    self.logger.info(f"### {self.application_name} status : {job_status_update}")

                    # Note: This is a very slow call with potential to slow the overall process

                    if loop_count % 24 == 0:
                        self.logger.write_metric_value("total_retrieved", self.talk_walker.total_item_count)
                        self.logger.write_metric_value("total_twitter", self.talk_walker.total_twitter_count)
                        self.logger.write_metric_value("twitter_errors", self.talk_walker.twitter_errors)
                        self.logger.write_metric_value("total_saved", self.talk_walker.total_saved)

                    if len(self.talk_walker.get_latest_errors()) != 0:
                        self.logger.info(f'{self.application_name} latest errors : {self.talk_walker.get_latest_errors()}')

            if tweet_items:
                merged_items = self.merge_tweet_data(tweet_items, error_file_path)
                self.talk_walker.total_saved += len(merged_items)
                self.save_data_to_file(merged_items, jsonl_file_path)

            self.logger.info(
                f"### {self.application_name} ### Final Total items retrieved: {self.talk_walker.total_item_count}"
            )
            self.logger.info(
                f"### {self.application_name} Final Total twitter items: {self.talk_walker.total_twitter_count}"
            )
            self.logger.info(
                f"### {self.application_name} ### Final Total items saved: {self.talk_walker.total_saved}"
            )
            self.logger.info(
                f"### {self.application_name} ### Total TalkWalker Items: {self.talk_walker.required_credits}"
            )

            # final update of job metrics
            self.logger.write_metric_value("total_retrieved", self.talk_walker.total_item_count)
            self.logger.write_metric_value("total_twitter", self.talk_walker.total_twitter_count)
            self.logger.write_metric_value("twitter_errors", self.talk_walker.twitter_errors)
            self.logger.write_metric_value("total_saved", self.talk_walker.total_saved)
            self.logger.info(f'{self.application_name} latest errors : {self.talk_walker.get_latest_errors()}')

            self.logger.info(f'{self.application_name} Status : talkwalker portion of the job is completed. Next step is to save results to S3 now.')

            # object_storage_key_for_results
            s3_jsonl_key_name = f"p6m/public/raw/{Constants.APPLICATION_NAME}/{self.talk_walker.project_id}/{topic_id}/{timestamp}/{task_id}.jsonl"
            self.upload_file(
                jsonl_file_path, self.buckets['output'], s3_jsonl_key_name
            )
#             original_document_metadata = self.put_original_document_metadata(
#                 task_queue_id=task_id,
#                 task_type=Constants.DRIVER_NAME,
#                 task_agent=task.get("claimed_by_agent", agent_name),
#                 original_document_uri=f"s3://{self.buckets['output']}/{s3_jsonl_key_name}",
#                 metadata={},
#             )
#             self.logger.info(
#                 f"original_document_metadata: {original_document_metadata}"
#             )

            self.logger.info(f'{self.application_name} Status : output bucket - has been written to.')


            jsonl_filename_txt = f"{jsonl_filename}.txt"
            self.extract_text(jsonl_filename, jsonl_file_path, jsonl_filename_txt)

#             put_text_document_metadata = self.put_text_document_metadata(
#                 task_queue_id=task_id,
#                 task_type=Constants.DRIVER_NAME,
#                 task_agent=task.get("claimed_by_agent", agent_name),
#                 original_document_uri=f"s3://{self.buckets['output']}/{s3_jsonl_key_name}",
#                 text_document_uri=jsonl_filename_txt,
#                 file_metadata={},
#                 page_metadata={},
#             )
#             self.logger.info(f"text_document_metadata: {put_text_document_metadata}")
#
#             self.logger.info(f'Status : output bucket - part 2 - has been written to.')

            # task_completed = self.task_completed(
            #     object_storage_key_for_results=f"s3://{self.buckets['output']}/{s3_jsonl_key_name}",
            #     task_id=task_id,
            #     success=True,
            #     message="Task completed - Platform version",
            # )
            # if "message" in task_completed:
            #     self.logger.info(task_completed["message"])

            data = {"output": f"{self.buckets['output']}/{s3_jsonl_key_name}"}
            airflow_output_file = "/airflow/xcom/return.json"

            with open(airflow_output_file, "w") as file:
                json.dump(data, file)

            self.logger.info(f'{self.application_name} Status : task completion been written to.')

            self.logger.info(f"{self.application_name} Job Id id = {task_id} completed.")
            self.logger.info(f"\n=========================================\n")
            self.logger.info(
                f"\n{self.application_name} == Results for Job id {task_id} is available at s3://{self.buckets['output']}/{s3_jsonl_key_name}  ==\n")
            self.logger.info(f"\n===============Completed=================\n")

        except (KeyboardInterrupt, TypeError, Exception) as e:

            # self.task_completed(
            #     object_storage_key_for_results=None,
            #     task_id=task_id,
            #     success=False,
            #     message="Task not completed - uncaught exception",
            # )
            print(traceback.format_exc())
            self.logger.error(traceback.format_exc())
            self.logger.info(f"Task failed - exception caught : {e}")
            exit(1)

        self.terminate_job_logger()
        exit(0)
