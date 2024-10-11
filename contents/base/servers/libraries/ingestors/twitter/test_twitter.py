import unittest
from unittest.mock import Mock, patch
from .twitter_ingestor import Twitter


class TestTwitter(unittest.TestCase):
    def setUp(self):
        self.config = Mock()
        self.twitter = Twitter("Twitter Dev")

    @patch("twitter.requests.get")
    def test_get_tweets_by_ids_successful_response(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": ["tweet_data"]}
        mock_get.return_value = mock_response

        items = [123, 456]
        data, twitter_error = self.twitter.get_tweets_by_ids(items)

        self.assertEqual(data, [("tweet_data",)])
        self.assertEqual(twitter_error, 0)

    @patch("twitter.requests.get")
    def test_get_tweets_by_ids_unsuccessful_response(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Error message"
        mock_get.return_value = mock_response

        items = [123, 456]
        data, twitter_error = self.twitter.get_tweets_by_ids(items)

        self.assertEqual(data, [])
        self.assertEqual(twitter_error, 1)

    @patch("twitter.requests.request")
    def test_connect_to_recent_search_endpoint_successful_response(self, mock_request):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": ["tweet_data"]}
        mock_request.return_value = mock_response

        endpoint_url = "https://api.twitter.com/2/tweets/search/recent"
        headers = {"Authorization": "Bearer token"}
        parameters = {"query": "test", "tweet.fields": "fields", "max_results": 10}

        json_response = self.twitter.connect_to_recent_search_endpoint(
            endpoint_url, headers, parameters
        )

        self.assertEqual(json_response, {"data": ["tweet_data"]})

    @patch("twitter.requests.request")
    def test_connect_to_recent_search_endpoint_unsuccessful_response(
        self, mock_request
    ):
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Error message"
        mock_request.return_value = mock_response

        endpoint_url = "https://api.twitter.com/2/tweets/search/recent"
        headers = {"Authorization": "Bearer token"}
        parameters = {"query": "test", "tweet.fields": "fields", "max_results": 10}

        with self.assertRaises(Exception):
            self.twitter.connect_to_recent_search_endpoint(
                endpoint_url, headers, parameters
            )


if __name__ == "__main__":
    unittest.main()