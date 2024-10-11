import logging
from bs4 import BeautifulSoup


class Html2Text:
    def __init__(self):
        super().__init__()
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

        self.input_file_name = None
        self.output_file_name = None

    def configure(self, input_file_name: str, output_file_name: str):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def convert(self) -> bool:
        if self.input_file_name is None or self.output_file_name is None:
            self.logger.info("Html2Text.configure() call is missing.")
            return False

        try:
            self.logger.info(f"Converting HTML from {self.input_file_name}.")

            with open(self.input_file_name, "r") as html_file:
                soup = BeautifulSoup(html_file, "html.parser")
                text = soup.get_text()

            lines = text.split("\n")
            formatted_text = "\n".join(lines)

            with open(self.output_file_name, "w") as text_file:
                text_file.write(formatted_text)

            self.logger.info(f"{self.output_file_name} is written completely.")
            return True

        except Exception as e:
            self.logger.error(e)
            return False