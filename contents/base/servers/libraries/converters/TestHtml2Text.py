import unittest
import os
from bs4 import BeautifulSoup
from html2text import Html2Text
import logging

class TestHtml2Text(unittest.TestCase):

    def setUp(self):
        self.html2text = Html2Text()
        self.input_file_path = "data/sample.html"
        self.output_file_path = "data/html_output.txt"
        logging.disable(logging.CRITICAL)  # Disable logging during tests

    def tearDown(self):
        logging.disable(logging.NOTSET)  # Re-enable logging

    def test_configure(self):
        self.html2text.configure(self.input_file_path, self.output_file_path)
        self.assertEqual(self.html2text.input_file_name, self.input_file_path)
        self.assertEqual(self.html2text.output_file_name, self.output_file_path)

    def test_convert_success(self):
        self.html2text.configure(self.input_file_path, self.output_file_path)
        
        result = self.html2text.convert()

        self.assertTrue(result)
        

    def test_convert_missing_config(self):
        result = self.html2text.convert()
        self.assertFalse(result)

    def test_convert_exception(self):
        self.html2text.configure("nonexistantfile.html", self.output_file_path)

        result = self.html2text.convert()
        self.assertFalse(result)

if __name__ == "__main__":
    unittest.main()