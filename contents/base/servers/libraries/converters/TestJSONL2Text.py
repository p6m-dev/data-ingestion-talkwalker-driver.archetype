import unittest
import os
from jsonl2text import JSONL2Text

class TestJSONL2Text(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = "data"
        print("setup")

    def tearDown(self):
        # Remove the temporary directory and its contents
        print("tear down")

    def test_conversion(self):
        # Create a test JSONL file
        input_file_name = os.path.join(self.test_dir, "sample.jsonl")
        output_file_name = os.path.join(self.test_dir, "samplejsonl_output.txt")

        

        # Configure and perform conversion
        jsonl_converter = JSONL2Text()
        jsonl_converter.configure(input_file_name, output_file_name)
        result = jsonl_converter.convert()

        # Check if conversion was successful
        self.assertTrue(result)
        self.assertTrue(os.path.exists(output_file_name))


    

if __name__ == "__main__":
    unittest.main()