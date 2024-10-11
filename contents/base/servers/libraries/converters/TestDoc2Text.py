import unittest
import logging


from doc2text import Doc2Text  

class TestDoc2Text(unittest.TestCase):

    def setUp(self):
        self.input_file_path = "./data/docxsample.docx"
        self.output_file_path = "./data/docx_output.txt"
        self.doc2text = Doc2Text()
        logging.disable(logging.CRITICAL)  # Disable logging during tests

    def tearDown(self):
        logging.disable(logging.NOTSET)  # Re-enable logging

    def test_configure(self):
        self.doc2text.configure(self.input_file_path,self.output_file_path)
        self.assertEqual(self.doc2text.input_file_name, self.input_file_path)
        self.assertEqual(self.doc2text.output_file_name, self.output_file_path)

    def test_convert_missing_configuration(self):
        self.assertFalse(self.doc2text.convert())

    def test_convert_successful(self):
        self.doc2text.configure(self.input_file_path, self.output_file_path)
        result = self.doc2text.convert()
        self.assertTrue(result)
        # Add assertions to check if the conversion was successful, e.g. by reading the output file

    def test_convert_exception(self):
        self.doc2text.configure("nonexistent.docx", "output.txt")
        result = self.doc2text.convert()
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()