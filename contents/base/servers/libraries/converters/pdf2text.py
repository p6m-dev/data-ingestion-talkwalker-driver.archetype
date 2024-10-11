import logging
from . converter import Converter

import fitz
import PyPDF2
import json
import os
import re
from datetime import datetime

class PDF2Text(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.input_file_name = None
        self.output_file_name = None
        self.is_page_wise_output = False
        self.is_metadata_included = False
        self.meta_output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self, input_file_name: str, output_file_name: str, is_page_wise_output:bool = False, is_metadata_included:bool = False):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.is_page_wise_output = is_page_wise_output
        self.is_metadata_included = is_metadata_included    
        # Automatically generate meta_output_file_name based on output_file_name
        base_name, ext = os.path.splitext(output_file_name)
        self.meta_output_file_name = f"{base_name}_meta.json"

    def convert(self) -> bool:
        # check if requirements are met
        if self.input_file_name is None or self.output_file_name is None or self.meta_output_file_name is None:
            self.logger.info("PDF2Text.configure() call is missing.")
            return False

        try:
            self.logger.info(f"converting PDF from {self.input_file_name}.")

            output_files = []
            
            # Open the PDF file using PyMuPDF
            pdf_document = fitz.open(self.input_file_name)
            
            # Initialize an empty string to store the extracted text
            extracted_text = ""
            
            # Iterate through each page of the PDF
            for page_number in range(pdf_document.page_count):
                page = pdf_document.load_page(page_number)
                
                # Extract text from the current page and append to the extracted_text string
                page_text = page.get_text("text")
                

                if(self.is_page_wise_output):
                    # Save the extracted text to a text file
                    output_name, ext = os.path.splitext(self.output_file_name)
                    file_output_file_name = f"{output_name}_{page_number}{ext}"
                    output_files.append(file_output_file_name)
                    with open(file_output_file_name, "w", encoding="utf-8") as text_file:
                        text_file.write(page_text)
                else:
                    extracted_text += page_text + "\n"  # Add a line break
                
            # Close the PDF document
            pdf_document.close()
            
            if(not self.is_page_wise_output):
                # Save the extracted text to a text file
                output_files.append(self.output_file_name)
                with open(self.output_file_name, "w", encoding="utf-8") as text_file:
                    text_file.write(extracted_text)
            
            self.logger.info(f"{self.output_file_name} is written completely.")
            
            if(self.is_metadata_included):
                # Extract metadata using PyPDF2
                with open(self.input_file_name, "rb") as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    metadata = pdf_reader.metadata
                    
                    # Convert PdfObject if in "D:YYYYMMDDHHMMSS" format
                    for key, value in metadata.items():
                        if isinstance(value, PyPDF2.generic.TextStringObject) and re.match(r'^D:\d{14}$', value):
                            pdf_datetime = datetime.strptime(value[2:], "%Y%m%d%H%M%S")
                            formatted_date = pdf_datetime.strftime("%Y-%m-%d %H:%M:%S")
                            metadata[key] = PyPDF2.generic.create_string_object(formatted_date)
                    
                # Save metadata as JSON
                with open(self.meta_output_file_name, "w", encoding="utf-8") as meta_file:
                    json.dump(metadata, meta_file, indent=4)
                
            self.logger.info(f"Metadata saved to {self.meta_output_file_name}.")
            

            return {
                "output_files": output_files,
                "meta_output_file_name": self.meta_output_file_name}

        except Exception as e:
            logging.error(e)
            return False

def main():
    input_file = "data/sample.pdf"  # Replace with your input PDF file path
    output_file = "data/pptsample_output.txt"  # Replace with your desired output text file path
    
    pdf_converter = PDF2Text()
    pdf_converter.configure(input_file, output_file, is_page_wise_output=True, is_metadata_included=True)
    
    print(pdf_converter.convert())

if __name__ == "__main__":
    main()