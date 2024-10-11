import logging
import subprocess
import json
import os
from . converter import Converter

from docx2txt import process
from docx import Document
from datetime import datetime

class Doc2Text(Converter):
    def __init__(self):
        Converter.__init__(self)
        self.input_file_name = None
        self.output_file_name = None
        self.meta_output_file_name = None
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    def configure(self, input_file_name: str, output_file_name: str):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        # Automatically generate meta_output_file_name based on output_file_name
        base_name, ext = os.path.splitext(output_file_name)
        self.meta_output_file_name = f"{base_name}_meta.json"

    def convert(self) -> bool:
        # check if requirements are met
        if self.input_file_name is None or self.output_file_name is None:
            self.logger.info("Doc2Text.configure() call is missing.")
            return False

        try:
            self.logger.info(f"Converting {self.input_file_name} to DOCX.")

            # Check if the input file is in DOC format
            if self.input_file_name.lower().endswith('.doc'):
                print(self.input_file_name)
                self.input_file_name = self.convert_doc_to_docx(self.input_file_name)

            # Process the DOCX file
            text = process(self.input_file_name)
            
            with open(self.output_file_name, 'w', encoding='utf-8') as txt_file:
                txt_file.write(text)


            
             # Extract metadata from DOCX file
            doc = Document(self.input_file_name)
            # extracted_metadata = {
            #     "title": doc.core_properties.title,
            #     "author": doc.core_properties.author,
            #     "subject": doc.core_properties.subject,
            #     "keywords": doc.core_properties.keywords,
            #     "created": doc.core_properties.created.strftime('%Y-%m-%d %H:%M:%S') if doc.core_properties.created is not None else "",
            #     "modified": doc.core_properties.modified.strftime('%Y-%m-%d %H:%M:%S') if doc.core_properties.modified is not None else "",
            # }
            extracted_metadata = {}
            for attr in dir(doc.core_properties):
                if not attr.startswith("_"):
                    try:
                        value = getattr(doc.core_properties, attr)
                        if value is not None:
                            if isinstance(value, (datetime,)):
                                value = value.strftime('%Y-%m-%d %H:%M:%S')
                            extracted_metadata[attr] = value
                    except AttributeError:
                        pass

                    

            print(extracted_metadata)
            

            # Save metadata in a JSON file
            json_metadata_file = self.meta_output_file_name
            with open(json_metadata_file, 'w', encoding='utf-8') as json_file:
                json.dump(extracted_metadata, json_file, indent=4)

            self.logger.info(f"{self.output_file_name} is written completely.")

            return True

        except Exception as e:
            self.logger.error(e)
            return False
    
    def convert_doc_to_docx(self, doc_file_path):
        # Use subprocess to call the unoconv command
        print(doc_file_path)
        
        # # Construct the output path for the converted DOCX file
        # converted_docx_file_path = doc_file_path.replace(".doc", ".docx")
        # print(converted_docx_file_path)
        # Use subprocess to call the unoconv command with the output path
        subprocess.call(["unoconv", "-f", "docx", doc_file_path])
        
        return doc_file_path.replace(".doc", ".docx")
        # # Rename the converted file to the desired output path
        # os.rename(converted_docx_file_path, doc_file_path)


def main():
    input_file = "data/docxsample.docx"  # Replace with your input PDF file path
    output_file = "data/docsample_output.txt"  # Replace with your desired output text file path
    
    pdf_converter = Doc2Text()
    pdf_converter.configure(input_file, output_file)
    
    if pdf_converter.convert():
        print("Conversion successful!")
    else:
        print("Conversion failed.")

if __name__ == "__main__":
    main()