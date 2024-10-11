import unittest
from pdf2text import PDF2Text

class TestPDF2Text(unittest.TestCase):

    def setUp(self):
        self.pdf2text = PDF2Text()
        self.input_file_name = "./data/sample.pdf"
        self.output_file_name = "./data/pdf_output.txt"

    def test_configure(self):
        self.pdf2text.configure(self.input_file_name, self.output_file_name)
        self.assertEqual(self.pdf2text.input_file_name, self.input_file_name)
        self.assertEqual(self.pdf2text.output_file_name, self.output_file_name)

    def test_convert_success(self):
        self.pdf2text.configure(self.input_file_name, self.output_file_name)
        result = self.pdf2text.convert()
        
        self.assertTrue(result)


    def test_convert_missing_configure(self):
        result = self.pdf2text.convert()
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()