import logging


class Ingestor:
    def __init__(self):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()

    @staticmethod
    def convert_epoch_to_unix(epoch_timestamp):
        try:
            # Find the length of the input epoch timestamp
            length = len(str(epoch_timestamp))
            # return 0 if there is no published date available from talkwalker
            if length < 1:
                return 0
            # raise error if there is no published date is not valid
            if length < 10:
                raise ValueError("Epoch timestamp is too short")

            # Conversion factor for seconds (epoch is in seconds)
            seconds_factor = 10 ** (length - 10)

            # Convert to seconds
            seconds = epoch_timestamp / seconds_factor

            return int(seconds)
        except Exception as e:
            # print(e)
            return -1