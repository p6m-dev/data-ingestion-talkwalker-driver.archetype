import os, json, random, time
import boto3
import logging
import requests
import nltk
from pprint import pprint
from fake_useragent import UserAgent
from datetime import date, timedelta
from datetime import datetime
from types import SimpleNamespace
from urllib.parse import urlparse
from newspaper import Article
from libraries.logs.cloudlogs import CloudMultiLogMetrics

from libraries.ingestors.ingestor import Ingestor


# from config import Config


# Config manifest
# MAX_RETRIES = 5


class TalkWalker(Ingestor):
    def __init__(self, params: dict, logger: CloudMultiLogMetrics):
        Ingestor.__init__(self)
        self.max_retries = int(os.getenv("MAX_RETRIES"))
        self.total = 0  # total items per request
        self.total_twitter_count = 0
        self.twitter_errors = 0
        self.total_item_count = 0  # total items retrieved so fat
        self.total_saved = 0
        self.required_credits = 0

        self.latest_errors = []

        # talkwalker params
        self.access_token = os.getenv("API_KEY")
        self.project_id = params['project_id']
        self.topic_id = params['topic_id']
        self.start_date = params['from_date']
        self.end_date = params['to_date']
        self.get_news_links = params['get_news_links']

        self.page_size = os.getenv("PAGE_SIZE")
        self.parameters = {}

        timestamp = int(time.time())  # Generate a unique timestamp
        log_filename = f"talkwalker_{self.topic_id}_attribution_logs_{timestamp}.jsonl"  # Include timestamp in the filename
        self.logger = logger
        nltk.download("punkt")

    def log_error(self, error_message):
        self.latest_errors.append(error_message)
        if len(self.latest_errors) > 10:
            self.latest_errors.pop(0)

    def get_latest_errors(self):
        return self.latest_errors

    def download_as_object(self, url):
        ua = UserAgent()
        headers = {"User-Agent": ua.random}
        for i in range(self.max_retries):
            try:
                response = requests.get(
                    url, params=self.parameters, headers=headers, timeout=10
                )
                response_json = response.json()
                response.raise_for_status()

                x = json.loads(
                    response.content, object_hook=lambda d: SimpleNamespace(**d)
                )
                return {"data": x, "pagination": response_json.get("pagination", {})}
            except requests.exceptions.Timeout:
                self.logger.error(f"Request timed out. Attempt: {i + 1}")
                self.log_error(f"Request timed out. Attempt: {i + 1}")
                if (
                        i < self.max_retries - 1
                ):  # wait before retrying, but not after the last attempt
                    time.sleep(5 * (i + 1))
                else:
                    break
            except Exception as e:
                self.logger.error(f"{e}")
                self.log_error(f"{e}")

    @staticmethod
    def get_domain_name(url):
        # Parse the URL using urlparse
        parsed_url = urlparse(url)
        # Extract the netloc (network location) and split by '.' to get the domain components
        domain_components = parsed_url.netloc.split(".")
        # Return the second-to-last component, which is typically the main domain name
        return domain_components[-2] if len(domain_components) > 1 else ""

    def save_attribution_logs_to_file(self, data):
        with open(self.log_file_path, "a") as f:
            f.write(json.dumps(data) + "\n")

    def format_data_item(self, item, published):
        if getattr(item.data, "external_provider", "") == "twitter":
            source = "twitter"
            self.total_twitter_count += 1
        else:
            source = self.get_domain_name(getattr(item.data, "url", ""))

        data = {
            "title": getattr(item.data, "title", ""),
            "body": getattr(item.data, "content", ""),
            "external_id": getattr(item.data, "external_id", ""),
            "external_provider": getattr(item.data, "external_provider", ""),
            "url": getattr(item.data, "url", ""),
            "lang": getattr(item.data, "lang", ""),
            "post_type": getattr(item.data, "post_type", ""),
            "sentiment": getattr(item.data, "sentiment", ""),
            "word_count": getattr(item.data, "word_count", ""),
            "engagement": getattr(item.data, "engagement", ""),
            "reach": getattr(item.data, "reach", ""),
            "published": published,
            "source": source,
            "source_type": getattr(item.data, "source_type", ""),
            "extra_author_attributes": self.nested_namespace_to_dict(
                getattr(item.data, "extra_author_attributes", "")
            ),
            "extra_source_attributes": self.nested_namespace_to_dict(
                getattr(item.data, "extra_source_attributes", "")
            ),
            "tokens_hashtag": getattr(item.data, "tokens_hashtag", ""),
            "article_extended_attributes": self.nested_namespace_to_dict(
                getattr(item.data, "article_extended_attributes", "")
            ),
            "source_extended_attributes": self.nested_namespace_to_dict(
                getattr(item.data, "source_extended_attributes", "")
            ),
            "tags_internal": getattr(item.data, "tags_internal", ""),
            "porn_level": getattr(item.data, "porn_level", ""),
            "fluency_level": getattr(item.data, "fluency_level", ""),
            "images": self.nested_namespace_to_dict(getattr(item.data, "images", "")),
            "videos": self.nested_namespace_to_dict(getattr(item.data, "videos", "")),
            "root_url": getattr(item.data, "root_url", ""),
            "parent_url": getattr(item.data, "parent_url", ""),
        }

        if published != 0 or published != -1:
            data["x-p6m-publish-source"] = "talkwalker"

        sources_to_check = [
            "BLOG_OTHER",
            "ONLINENEWS",
            "ONLINENEWS_AGENCY",
            "ONLINENEWS_MAGAZINE",
            "ONLINENEWS_NEWSPAPER",
            "ONLINENEWS_OTHER",
            "ONLINENEWS_PRESSRELEASES",
            "ONLINENEWS_TVRADIO",
            "PODCAST_OTHER",
        ]
        is_news = any(
            element in getattr(item.data, "source_type", "")
            for element in sources_to_check
        )
        if is_news and self.get_news_links:

            article_dict = {}
            attributions = {}
            attributions["url"] = getattr(item.data, "url", "")
            attributions["source"] = (source,)
            attributions["snippet"] = True
            try:
                url = getattr(item.data, "url", "")
                article = Article(
                    url=url,
                    # language=getattr(item.data, "lang", ""),
                )
                self.logger.info(f"Fetching Article {url}")
                article.download()
                article.parse()
                article.nlp()
                # print(article)

                if article.publish_date is not None:
                    article_dict["datetime"] = article.publish_date.isoformat()
                else:
                    article_dict[
                        "datetime"
                    ] = None  # Or any other default value you prefer
                article_dict["media"] = source
                article_dict["title"] = article.title
                article_dict["authors"] = article.authors
                article_dict["text"] = article.text
                article_dict["summary"] = article.summary
                article_dict["url"] = article.url

                # print("dict#: ", dict)
                data["news_article"] = article_dict
                attributions["successful_traversal"] = True

            except Exception as e:
                attributions["successful_traversal"] = f"{e}"
                self.logger.info(e)
                self.logger.info("Ignoring this article")
                article_url = attributions["url"]
                self.log_error(f"error: {e} article: {article_url}")

            try:
                self.save_attribution_logs_to_file(attributions)
            except Exception as e:
                self.logger.info(e)
                self.log_error(f"error in article: {e}")
        return data

    def nested_namespace_to_dict(self, obj):
        if isinstance(obj, SimpleNamespace):
            return {
                key: self.nested_namespace_to_dict(value)
                for key, value in obj.__dict__.items()
            }
        elif isinstance(obj, list):
            return [self.nested_namespace_to_dict(item) for item in obj]
        else:
            return obj

    def extract_offset_from_next(self, next_url):
        """
        Method to extract the next offset number from the next url params
        """
        offset_key = "offset="
        offset_start_index = next_url.find(offset_key)
        if offset_start_index != -1:
            offset_start_index += len(offset_key)
            offset_end_index = next_url.find("&", offset_start_index)
            if offset_end_index != -1:
                return int(next_url[offset_start_index:offset_end_index])
        return None

    def search_results(self, url):
        scrape_start_time = time.time()  # Record the start time of the scrape function
        items = []  # list to hold tweet items for batching

        while True:
            time.sleep(0.1)  # we are still getting rate limit 429s
            x = self.download_as_object(url)

            # print("==object downloaded==")
            # pprint(x)
            self.total = 0

            if x is None:
                # print("==skipping as x is None==")
                break

            content = x.get("data").result_content
            if content is None:
                # print("==skipping as content is None==")
                break

            data = getattr(content, "data", None)
            if data is None:
                # print("==skipping as data is None==")
                break

            for item in data:
                self.total += 1

                published = self.convert_epoch_to_unix(
                    getattr(item.data, "published", "")
                )
                items.append(self.format_data_item(item, published))

            next_offset = self.extract_offset_from_next(
                x.get("pagination", {}).get("next", "")
            )
            if next_offset is None:
                break

            self.parameters["offset"] = next_offset

            # Check if the scrape function has run for more than 1 second
            if time.time() - scrape_start_time < 1:
                time.sleep(1)
        return items

    @staticmethod
    def get_epoch_time(day, month, year):
        date = datetime(year, month, day)
        epoch_time = int(time.mktime(date.timetuple()))
        return epoch_time

    def retrieve_data(self):
        start_time = time.time()  # Record the start time
        url = f"https://api.talkwalker.com/api/v1/search/p/{self.project_id}/results"

        total_twitter_error_count = 0
        # Calculate the start and end dates

        start_date = (
            datetime.strptime(self.start_date, "%Y-%m-%d")
            if self.start_date
            else datetime.today()
        )

        end_date = (
            datetime.strptime(self.end_date, "%Y-%m-%d")
            if self.end_date
            else start_date + timedelta(days=30)
        )

        # Swap start_date with end_date if start_date is greater
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        self.logger.info(f"starting search from {start_date} till {end_date}")

        # Loop through each day from the start_date to the end_date
        for n in range(int((end_date - start_date).days) + 1):
            current_day = start_date + timedelta(n)
            year = current_day.year
            month = current_day.month
            day = current_day.day

            start = self.get_epoch_time(day, month, year)
            self.logger.info(f"Fetching Day - {month}/{day}/{year}")
            # Loop through 24 hours with 1-hour intervals
            for i in range(24):
                # Calculate the end time, which is 1 hour apart from the start time
                end = start + 3600  # 3600 seconds = 1 hour

                self.parameters = {
                    "access_token": self.access_token,
                    "topic": self.topic_id,
                    "hpp": self.page_size,
                    "offset": 0,
                    "project_id": self.project_id,
                    "q": f"(published:>={start} AND published:<{end})",
                }
                self.logger.info(f"Fetching Hour - {month}/{day}/{year} - {i}")
                # yield self.search_results(url)
                yield self.search_results(url)

                # break
                self.total_item_count += self.total

                # Update the start time for the next interval
                start = end
                # self.logger.info(f"Fetching  {month}/{day}/{year} Hour = {i}")
                self.logger.info(
                    f"Item retrieved for {month}/{day}/{year} hour {i}: {self.total}"
                )
                self.logger.info(
                    f"\r### Total: {self.required_credits}, Remaining: {self.required_credits - self.total_item_count}  Retrieved: {self.total_item_count}, Saved: {self.total_saved}, Twitter: {self.total_twitter_count}, Twitter errors: {self.twitter_errors} ###"
                )

            # break

        end_time = time.time()  # Record the end time
        execution_time = str(timedelta(seconds=int(end_time - start_time)))
        # Log the final report
        self.logger.info(f"=========================================")
        self.logger.info(f"=========== Execution Summary ===========")

        self.logger.info(f"Execution Time: {execution_time}")
        self.logger.info(
            f"Number of Twitter Errors Encountered: {total_twitter_error_count}"
        )
        # self.logger.info(f"Total Items Collected: {total_item_count}")