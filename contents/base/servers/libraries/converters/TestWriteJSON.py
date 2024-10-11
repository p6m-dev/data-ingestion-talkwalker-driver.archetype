import unittest
import json
import os
import logging
from writejson import WriteJSON

class TestWriteJSON(unittest.TestCase):

    def setUp(self):
        
        logging.disable(logging.CRITICAL)  # Disable logging during tests
        self.input_file_path = "./data/sample.json"
        self.output_file_path = "./data/test_output.jsonl"
        self.json_converter = WriteJSON()

    def tearDown(self):
        logging.disable(logging.NOTSET)  # Re-enable logging

    def test_conversion(self):
        self.json_converter.configure(self.input_file_path, self.output_file_path)
        conversion_result = self.json_converter.convert()

        self.assertTrue(conversion_result)
        self.assertTrue(os.path.exists(self.output_file_path))

        

        

    def test_missing_configuration(self):
        conversion_result = self.json_converter.convert()
        self.assertFalse(conversion_result)

    def test_invalid_input_file(self):
        self.json_converter.configure("nonexistent_input.json", self.output_file_path)
        conversion_result = self.json_converter.convert()
        self.assertFalse(conversion_result)

if __name__ == '__main__':
    unittest.main()