import logging
from . converter import Converter
from bs4 import BeautifulSoup
import os
import re
import json



class Html2Text(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.input_file_name = None
        self.output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self,input_file_name: str, output_file_name: str):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def convert(self) -> bool:
        # check if requirements are met
        if self.input_file_name is None or self.output_file_name is None:
            self.logger.info("Html2Text.configure() call is missing.")
            return False

        try:
            self.logger.info(f"converting html from {self.input_file_name}.")


            with open(self.input_file_name, 'r') as html_file:
                soup = BeautifulSoup(html_file, 'html.parser')
                text = soup.get_text()

            lines = text.split("\n")  # Split text into lines and add line breaks

            formatted_text = "\n".join(lines)
            
            # with open(self.output_file_name, 'w') as text_file:
            #     text_file.write(formatted_text)

            self.logger.info(f"{self.output_file_name} is written completely.")

            metadata = {}
            # Find and add author from <meta> tag
            author_tag = soup.find("meta", {"name": "author"})
            if author_tag:
                metadata["author"] = author_tag.get("content", None)

            # Attempt to find created/modified date from various sources
            date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
            date_matches = re.findall(date_pattern, formatted_text)
            if date_matches:
                metadata["created_at"] = date_matches[0]
                # You can also attempt to find modified date by checking other matches if needed

            with open(self.output_file_name, 'w') as text_file:
                json.dump(metadata, text_file, indent=4)


            return True

        except Exception as e:
            logging.error(e)
            return False





def main():
    input_file = "data/sample.html"  # Replace with your input PDF file path
    output_file = "data/htmlsample_output.json"  # Replace with your desired output text file path
    
    pdf_converter = Html2Text()
    pdf_converter.configure(input_file, output_file)
    
    if pdf_converter.convert():
        print("Conversion successful!")
    else:
        print("Conversion failed.")

if __name__ == "__main__":
    main()