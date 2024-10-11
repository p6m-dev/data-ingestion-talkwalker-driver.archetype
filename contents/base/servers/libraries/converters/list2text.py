import logging
from .converter import Converter


class List2Text(Converter):
    def __init__(self):
        # initialize super class
        Converter.__init__(self)

        self.output_file_name = None

        # configure logging

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )

        self.logger = logging.getLogger()

    def configure(self, output_file_name: str):
        self.output_file_name = output_file_name

    def convert(self, lines: list) -> bool:
        # check if requirements are met
        if self.output_file_name is None:
            self.logger.info("List2Text.configure() call is missing.")
            return False

        with open(self.output_file_name, "w") as output:
            for line in lines:
                output.write(f"{line}\n")

        self.logger.info(f"text output is written to {self.output_file_name}.")

        return True