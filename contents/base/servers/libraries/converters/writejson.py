import logging
from converter import Converter
import json




class WriteJSON(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.input_file_name = None
        self.output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self, input_file_name: str, output_file_name: str):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def convert(self) -> bool:
        # check if requirements are met
        if self.input_file_name is None or self.output_file_name is None:
            self.logger.info("JsonArrayToJSONL.configure() call is missing.")
            return False

        try:
            self.logger.info(f"Converting JSON array from {self.input_file_name} to JSONL.")

            with open(self.input_file_name, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)

            with open(self.output_file_name, 'w', encoding='utf-8') as jsonl_file:
                for item in data:
                    jsonl_file.write(json.dumps(item) + '\n')

            self.logger.info(f"{self.output_file_name} is written in JSONL format.")

            return True

        except Exception as e:
            logging.error(e)
            return False

