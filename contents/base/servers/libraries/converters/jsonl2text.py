import logging
from . converter import Converter

import json
import os

class JSONL2Text(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.input_file_name = None
        self.output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self, input_file_name, output_file_name):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def convert(self):
        if not self.input_file_name or not self.output_file_name:
            self.logger.info("JSONL2TextConverter.configure() call is missing.")
            return False

        try:
            self.logger.info(f"converting JSONL from {self.input_file_name}.")

            with open(self.input_file_name, "r", encoding="utf-8") as jsonl_file:
                lines = jsonl_file.readlines()
                extracted_text = ""

                for line in lines:
                    data = json.loads(line)
                    if "text" in data:
                        extracted_text += data["text"] + "\n"

            with open(self.output_file_name, "w", encoding="utf-8") as text_file:
                text_file.write(extracted_text)

            self.logger.info(f"{self.output_file_name} is written completely.")
            return True

        except Exception as e:
            print(e)
            return False

def main():
    input_file = "data/sample.jsonl"  # Replace with your input JSONL file path
    output_file = "data/samplejson_output.txt"  # Replace with your desired output text file path

    jsonl_converter = JSONL2Text()
    jsonl_converter.configure(input_file, output_file)

    if jsonl_converter.convert():
        print("Conversion successful!")

if __name__ == "__main__":
    main()