import logging
import whisper
from .converter import Converter
from nltk.tokenize import sent_tokenize


class Speech2List(Converter):
    """This class uses openai whisper model to convert a speech file to text"""

    def __init__(self):
        """constructor"""

        Converter.__init__(self)
        self.model_type = None
        self.input_file_name = None

        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()
        # self.logger.setLevel(logging.ERROR)

    def configure(self, input_file_name: str, model_type: str):
        """configure the conversion parameters"""

        self.model_type = model_type
        self.input_file_name = input_file_name

    @staticmethod
    def split_string(s: str) -> list:
        """Splits a sentence properly using NLP techniques"""

        return sent_tokenize(s)

    def convert(self) -> list:
        """This method does speech to text conversion"""

        rc = None

        try:
            self.logger.info(f"loading {self.model_type} model.")

            model = whisper.load_model(self.model_type)

            self.logger.info(f"{self.model_type} model loading is completed.")

            self.logger.info(f"starting transcription of {self.input_file_name} file.")

            result = model.transcribe(self.input_file_name)

            self.logger.info("transcription completed.")

            # result is a string list
            rc = Speech2List.split_string(result["text"])
        except:
            rc = None
            self.logger.exception("transcription failed.")

        return rc