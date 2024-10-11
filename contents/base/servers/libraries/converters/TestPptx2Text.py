import unittest
import logging
from pptx2text import Pptx2Text

class TestPptx2Text(unittest.TestCase):

    def setUp(self):
        self.input_file_path = "./data/pptsample.ppt"
        self.output_file_path = "./data/ppt_output.txt"
        self.pptx2text = Pptx2Text()
        logging.disable(logging.CRITICAL)  # Disable logging during tests

    def tearDown(self):
        logging.disable(logging.NOTSET)  # Re-enable logging

    def test_configure(self):
        self.pptx2text.configure(self.input_file_path, self.output_file_path)
        self.assertEqual(self.pptx2text.input_file_name, self.input_file_path)
        self.assertEqual(self.pptx2text.output_file_name, self.output_file_path)

    def test_convert_missing_configuration(self):
        self.assertFalse(self.pptx2text.convert())

    def test_convert_successful(self):
        self.pptx2text.configure(self.input_file_path, self.output_file_path)
        result = self.pptx2text.convert()
        self.assertTrue(result)
        # Add assertions to check if the conversion was successful, e.g. by reading the output file

    def test_convert_exception(self):
        self.pptx2text.configure("nonexistent.pptx", "output.txt")
        result = self.pptx2text.convert()
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()