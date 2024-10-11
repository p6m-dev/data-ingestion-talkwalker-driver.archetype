import os
import io
import boto3
import json
import time
import requests
import logging
import pandas as pd
import pandasql as ps
from datetime import datetime
from libraries.ingestors.ingestor import Ingestor

from libraries.converters.edgarhtml2text import Html2Text

# Load AWS credentials and region from environment variables
aws_access_key_id = os.getenv("AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION")


data_dir = os.path.join("libraries/data/edgar")
done_file = os.path.join("libraries/data/edgar", "done.txt")


class EdgarIngestor(Ingestor):
    def __init__(self, inputs):
        """
        Initialize the EdgarIngestor.

        Args:
            inputs (dict): Dictionary containing input parameters.
        """
        Ingestor.__init__(self)
        self.ticker_url = "https://www.sec.gov/include/ticker.txt"
        self.from_date = inputs["from_date"]
        self.to_date = inputs["to_date"]
        self.form_type = inputs["form_type"]
        self.bucket = inputs["bucket"]
        self.user_agent = inputs["user_agent"]
        self.filing_date_Qtr = ""
        self.year = inputs["year"]
        self.s3_key = f"p6m/public/edgar/{self.form_type}/{self.year}"
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def build_api_url(self, category, filename):
        """
        Build the API URL for SEC data.

        Args:
            category (str): Data category.
            filename (str): Filename.

        Returns:
            str: The constructed API URL.
        """
        return f"https://data.sec.gov/{category}/{filename}"

    def get_ticker_details(self):
        """
        Get ticker details from SEC.

        Returns:
            pd.DataFrame: A DataFrame containing ticker details.
        """
        done_ciks = set()

        if os.path.exists(done_file):
            with open(done_file, "r") as f:
                done_ciks = set(f.read().splitlines())

        headers = {"cache-control": "no-cache", "User-Agent": self.user_agent}
        response = requests.get(self.ticker_url, headers=headers)
        ticker_data = pd.read_csv(io.StringIO(response.text), sep="\t", header=None)
        ticker_data = ticker_data.sort_values(by=1)
        ticker_data = ticker_data[~ticker_data[1].astype(str).isin(done_ciks)]
        return ticker_data

    def process_submissions(self, ticker_details):
        """
        Process SEC submissions for ticker details.

        Args:
            ticker_details (pd.DataFrame): DataFrame containing ticker details.
        """
        hit_count = 0

        for index, row in ticker_details.iterrows():
            cikid = str(row[1]).zfill(10)
            json_filename = f"CIK{cikid}.json"
            base_url = self.build_api_url("submissions", json_filename)
            self.logger.info(f"url for cik: {base_url}")
            headers = {"cache-control": "no-cache", "User-Agent": self.user_agent}

            response = requests.get(base_url, headers=headers)
            self.process_form_details(response, cikid)

            hit_count += 1

            if hit_count % 100 == 0:
                self.logger.info("Sleeping for 2 minutes to rate limit...")
                time.sleep(120)

    def upload_to_s3(self, file_content, s3_key):
        """
        Upload file content to AWS S3.

        Args:
            file_content (bytes): File content to upload.
            s3_key (str): S3 key to store the file.
        """
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        try:
            s3.put_object(Body=file_content, Bucket=self.bucket, Key=s3_key)
            self.logger.info(f"Uploaded data to S3 bucket: s3://{self.bucket}/{s3_key}")
        except Exception as e:
            self.logger.error(f"Error uploading data to S3 bucket: {str(e)}")

    def download_files(self, row, cik):
        """
        Download files from SEC and upload to S3.

        Args:
            row (pd.Series): DataFrame row containing filing details.
            cik (str): CIK identifier.
        """
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{row['accessionNumber'].replace('-', '')}/{row['primaryDocument']}"
        headers = {"cache-control": "no-cache", "User-Agent": self.user_agent}
        response = requests.get(base_url, headers=headers)
        file_content = response.content

        file_name = f"{row['primaryDocument']}"
        file_path = f"{data_dir}/{row['primaryDocument']}"
        filing_date = row["filingDate"].strftime("%Y-%m-%d")

        with open(file_path, "wb") as f:
            f.write(file_content)

        split_tup = str(row["primaryDocument"]).split(".")
        file_extension = split_tup[1].lower()
        text_file_name = split_tup[0] + ".txt"
        text_file_path = f"{data_dir}/{text_file_name}"

        if self.form_type == "10-Q" and filing_date != "":
            filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
            filing_month = filing_dt.month
            filing_quarter = ""

            if filing_month in [1, 2, 3]:
                filing_quarter = "Q1"
            elif filing_month in [4, 5, 6]:
                filing_quarter = "Q2"
            elif filing_month in [7, 8, 9]:
                filing_quarter = "Q3"
            elif filing_month in [10, 11, 12]:
                filing_quarter = "Q4"
            else:
                filing_quarter = "Invalid_month"

            s3_key = (
                f"{self.s3_key}/{filing_quarter}/{cik.lstrip('0')}/{text_file_path}"
            )

        else:
            s3_key = f"{self.s3_key}/{cik.lstrip('0')}/{text_file_path}"

        if file_extension == "htm" or file_extension == "html":
            html_converter = Html2Text()
            html_converter.configure(file_path, text_file_path)
            success = html_converter.convert()

            if not success:
                self.logger.error(f"Failed to convert file {file_name}")

            if success:
                if os.path.isfile(text_file_name):
                    with open(file_path, "r") as fi:
                        self.upload_to_s3(fi.read(), s3_key)

                if os.path.isfile(file_path):
                    if self.form_type == "10-Q" and filing_date != "":
                        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
                        filing_month = filing_dt.month
                        filing_quarter = ""

                        if filing_month in [1, 2, 3]:
                            filing_quarter = "Q1"
                        elif filing_month in [4, 5, 6]:
                            filing_quarter = "Q2"
                        elif filing_month in [7, 8, 9]:
                            filing_quarter = "Q3"
                        elif filing_month in [10, 11, 12]:
                            filing_quarter = "Q4"
                        else:
                            filing_quarter = "Invalid_month"

                        s3_key_pd = f"{self.s3_key}/{filing_quarter}/{cik.lstrip('0')}/{row['primaryDocument']}"

                        self.key_value_pairs["Filing Date"] = filing_date
                        metadata_json_object = json.dumps(
                            self.key_value_pairs, indent=4
                        )

                        metadata_filename = f"{cik}.metadata"
                        with open(metadata_filename, "w") as outfile:
                            outfile.write(metadata_json_object)

                        s3_key_meta = f"{self.s3_key}/{filing_quarter}/{cik.lstrip('0')}/{metadata_filename}"

                        with open(metadata_filename, "rb") as metadata_file:
                            metadata_content = metadata_file.read()
                            self.upload_to_s3(metadata_content, s3_key_meta)

                    else:
                        s3_key_pd = (
                            f"{self.s3_key}/{cik.lstrip('0')}/{row['primaryDocument']}"
                        )

                    self.upload_to_s3(file_content, s3_key_pd)

                if os.path.isfile(file_path):
                    # os.remove(file_path)
                    pass
                if os.path.isfile(text_file_path):
                    # os.remove(text_file_path)
                    pass
        else:
            self.logger.warning("File extension is not html")

        with open(done_file, "a") as f:
            f.write(cik.lstrip("0") + "\n")

    def process_form_details(self, main_dec_res, cikid):
        """
        Process form details and download associated files.

        Args:
            main_dec_res (requests.Response): Main declaration response.
            cikid (str): CIK identifier.
        """
        main_json = main_dec_res.json()
        main_details_json = main_json["filings"]["recent"]

        main_df = pd.DataFrame.from_dict(main_details_json)
        main_df["filingDate"] = pd.to_datetime(main_df["filingDate"], format="%Y-%m-%d")

        from_date = datetime.strptime(self.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(self.to_date, "%Y-%m-%d")

        query = f"""SELECT * FROM main_df WHERE form='{self.form_type}' AND filingDate BETWEEN '{from_date}' AND '{to_date}' """
        filtered_df = ps.sqldf(query)
        date_str = ""
        if not filtered_df.empty:
            filtered_df["filingDate"] = pd.to_datetime(filtered_df["filingDate"])
            date_str = filtered_df["filingDate"].dt.strftime("%Y-%m-%d").iloc[0]
        else:
            print("No rows matching the query.")

        filtered_df.apply(self.download_files, cik=cikid, axis=1)

        key_value_pairs = {
            "Company Name": main_json["name"],
            "Company cik": main_json["cik"],
            "entity type": main_json["entityType"],
            "Sic": main_json["sic"],
            "Sic Description": main_json["sicDescription"],
            "Category": main_json["category"],
            "Fiscal Year End": main_json["fiscalYearEnd"],
            "Addresses": main_json["addresses"],
            "Filing Date": date_str,
        }

        metadata_json_object = json.dumps(key_value_pairs, indent=4)

        metadata_filename = f"{cikid}.metadata"
        metadata_filename_path = f"{data_dir}/{cikid}.metadata"

        with open(metadata_filename_path, "w") as outfile:
            outfile.write(metadata_json_object)

        s3_key = f"p6m/test/public/forms/edgar/{self.form_type}/{cikid.lstrip('0')}/{metadata_filename}"
        with open(metadata_filename_path, "rb") as metadata_file:
            metadata_content = metadata_file.read()
            self.upload_to_s3(metadata_content, s3_key)

        # os.remove(metadata_filename_path)

        with open(done_file, "a") as f:
            f.write(cikid.lstrip("0") + "\n")