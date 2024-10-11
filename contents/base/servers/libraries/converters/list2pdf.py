import fpdf
import logging
from .converter import Converter


class List2Pdf(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.lines_per_page = None
        self.output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self, output_file_name: str, lines_per_page: int):
        self.output_file_name = output_file_name
        self.lines_per_page = lines_per_page

    def convert(self, lines: list) -> bool:
        # check if requirements are met
        if self.lines_per_page is None or self.output_file_name is None:
            self.logger.info("List2Pdf.configure() call is missing.")
            return False

        try:
            self.logger.info(f"writing {len(lines)} lines to {self.output_file_name}.")

            sink = fpdf.FPDF()
            sink.add_page()
            sink.set_font("Arial", "", 10)

            line_count = 0

            with open(self.output_file_name, "w") as output:
                for line in lines:
                    line_count += 1

                    if line_count == self.lines_per_page:
                        line_count = 0
                        sink.add_page()

                    # output.write(f"{line}")
                    sink.cell(200, 8, txt=line, ln=1, align="L")

            sink.output(name=self.output_file_name, dest="F")

            self.logger.info(f"{self.output_file_name} is written completely.")

            return True

        except Exception as e:
            logging.error(e)

            return False