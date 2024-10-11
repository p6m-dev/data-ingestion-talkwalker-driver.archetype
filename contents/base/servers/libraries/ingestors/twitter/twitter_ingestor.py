import os, json, random, time
import boto3
import logging
import requests
from pprint import pprint

from libraries.ingestors.ingestor import Ingestor

# from config import Config


# Config manifest


class Twitter(Ingestor):
    def __init__(self):
        Ingestor.__init__(self)
        self.page_size = os.getenv("PAGE_SIZE")
        self.twitter_token = os.getenv("TWITTER_TOKEN")
        self.max_retries = int(os.getenv("MAX_RETRIES"))
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    @staticmethod
    def get_user_by_id(user_list, target_id):
        for item in user_list:
            if item["id"] == target_id:
                return item
        return ""

    def append_error_to_file(self, error, filename):
        with open(filename, "a") as f:
            f.write(error + "\n")

    def get_tweets_by_ids(self, items, error_file_path):
        """
        Method to send a bulk tweet api call and return the tweet information.
        returns a tuple of tweets data and twitter_error. The twitter_error is 1 if the api response is not successful.
        """

        api_url = "https://api.twitter.com/2/tweets"
        tweet_ids = ",".join(map(str, items))
        params = {
            "tweet.fields": "attachments,author_id,conversation_id,created_at,public_metrics,source,context_annotations,lang,referenced_tweets,in_reply_to_user_id,geo",
            "expansions": "author_id",
            "user.fields": "description,location,name,username,verified",
            "ids": f"{tweet_ids}",
        }
        headers = {"Authorization": f"Bearer {self.twitter_token}"}

        for i in range(self.max_retries):
            try:
                response = requests.get(
                    api_url, params=params, headers=headers, timeout=10
                )

                data = []
                response_data = response.json()

                error_collection = response_data.get("errors")
                if error_collection:
                    self.logger.error(
                        f"Twitter returned partial {len(error_collection)} errors."
                    )
                    for key in error_collection:
                        # self.logger.error(f"{key}")
                        self.append_error_to_file(f"{key}", error_file_path)

                # self.logger.info(f"response_data['errors'] = {response_data['errors']}")

                result = {"data": [], "errors": []}
                if response.status_code == 200:
                    if "data" in response_data:
                        for tweet_data in zip(response_data["data"]):
                            tweet_dict = dict(tweet_data[0])
                            if "includes" in response_data:
                                includes = response_data["includes"]
                                author = self.get_user_by_id(
                                    includes.get("users"), tweet_dict["author_id"]
                                )
                                tweet_dict["author"] = author
                            data.append(tweet_dict)
                    if "errors" in response_data:
                        result["errors"] = response_data["errors"]
                else:
                    logging.error(f"get_tweets_by_ids: {response.text}")

                result["data"] = data
                return result
            except requests.exceptions.Timeout:
                self.logger.error(f"Twitter timeout retry hit")
                self.logger.error(
                    f"get_tweets_by_ids - Request timed out. Attempt: {i+1}"
                )
                if (
                    i < self.max_retries - 1
                ):  # wait before retrying, but not after the last attempt
                    time.sleep(5 * (i + 1))
                else:
                    break
            except Exception as e:
                self.logger.error(f"{e}")

    def connect_to_recent_search_endpoint(
        self, endpoint_url: str, headers: dict, parameters: dict
    ) -> json:
        """
        Connects to the endpoint and requests data.
        Returns a json with Twitter data if a 200 status code is yielded.
        Programme stops if there is a problem with the request and sleeps
        if there is a temporary problem accessing the endpoint.
        """
        for i in range(self.max_retries):
            try:
                response = requests.request(
                    "GET", url=endpoint_url, headers=headers, params=parameters
                )
                response_status_code = response.status_code
                if response_status_code != 200:
                    if response_status_code >= 400 and response_status_code < 500:
                        raise Exception(
                            "Cannot get data, the program will stop!\nHTTP {}: {}".format(
                                response_status_code, response.text
                            )
                        )

                    sleep_seconds = random.randint(5, 20)
                    print(
                        "Cannot get data, your program will sleep for {} seconds...\nHTTP {}: {}".format(
                            sleep_seconds, response_status_code, response.text
                        )
                    )
                    time.sleep(sleep_seconds)
                    return self.connect_to_recent_search_endpoint(
                        endpoint_url, headers, parameters
                    )
                return response.json()
            except requests.exceptions.Timeout:
                self.logger.error(
                    f"connect_to_recent_search_endpoint - Request timed out. Attempt: {i+1}"
                )
                if (
                    i < self.max_retries - 1
                ):  # wait before retrying, but not after the last attempt
                    time.sleep(5 * (i + 1))
                else:
                    break
            except Exception as e:
                self.logger.error(f"{e}")

    def get_tweets_by_query(self, query):
        """
        Method to send a bulk tweet api call and return the tweet information.
        returns a tuple of tweets data and twitter_error. The twitter_error is 1 if the api response is not successful.
        """

        endpoint_url = "https://api.twitter.com/2/tweets/search/all"
        query_parameters = {
            "query": query,
            "tweet.fields": "attachments,author_id,conversation_id,created_at,public_metrics,source,context_annotations,lang,referenced_tweets,in_reply_to_user_id,geo",
            "expansions": "author_id",
            "user.fields": "description,location,name,username,verified",
            "max_results": self.page_size,
        }
        headers = {"Authorization": f"Bearer {self.twitter_token}"}
        data = []

        json_response = self.connect_to_recent_search_endpoint(
            endpoint_url, headers, query_parameters
        )
        if "data" in json_response.keys():
            for tweet_data in zip(json_response["data"]):
                tweet_dict = dict(tweet_data[0])
                if "includes" in json_response.keys():
                    includes = json_response["includes"]
                    author = self.get_user_by_id(
                        includes.get("users"), tweet_dict["author_id"]
                    )
                    tweet_dict["author"] = author
                yield tweet_dict
        time.sleep(5)

        while "next_token" in json_response["meta"]:
            query_parameters["next_token"] = json_response["meta"]["next_token"]
            json_response = self.connect_to_recent_search_endpoint(
                endpoint_url, headers, query_parameters
            )
            if "data" in json_response.keys():
                for tweet_data in zip(json_response["data"]):
                    tweet_dict = dict(tweet_data[0])
                    if "includes" in json_response.keys():
                        includes = json_response["includes"]
                        author = self.get_user_by_id(
                            includes.get("users"), tweet_dict["author_id"]
                        )
                        tweet_dict["author"] = author
                    yield tweet_dict
            time.sleep(5)

        return data