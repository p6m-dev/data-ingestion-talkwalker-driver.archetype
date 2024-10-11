import os
import time
import json
import logging
import pandas as pd
import requests
from libraries.drivers.driver import Driver
from libraries.ingestors.edgar.edgar_ingestor import EdgarIngestor


TASK_TYPE = "edgar"
AGENT_ID = f"edgar-{os.getpid()}"


class EdgarDriver(Driver):
    def __init__(self):
        """
        Initialize the EdgarDriver.
        """
        Driver.__init__(self)
        self.logger.setLevel(logging.INFO)
        self.sec_gov = None

    def process_submissions(self, inputs):
        if "cik" in inputs:
            companyList = []
            companyList.append("CMPNY")
            companyList.append(inputs["cik"])
            df_cik = pd.DataFrame([companyList])
            print(df_cik)
            self.sec_gov.process_submissions(df_cik)

        elif "ticker" in inputs:
            companyList = []
            companyName = str(inputs["ticker"]).upper()

            headers = {"User-Agent": "ubkadiyala@gmail.com"}
            companyCik = requests.get(
                "https://www.sec.gov/files/company_tickers.json", headers=headers
            )

            jsonList = json.loads(json.dumps(companyCik.json()))
            df_ticker_cik = 0
            for k, v in jsonList.items():
                if companyName in str(v):
                    cik_str = v["cik_str"]
                    companyList.append(companyName)
                    companyList.append(cik_str)
                    df_ticker_cik = pd.DataFrame([companyList])
            self.logger.info(f"ticker: {df_ticker_cik}")
            self.logger.info(f"companyList: {companyList}")
            self.sec_gov.process_submissions(df_ticker_cik)

        elif "text_file" in inputs:
            df = pd.read_csv(inputs["text_file"], sep="\t", header=None)
            print(df[1])
            for x in df[1]:
                companyList = []
                companyList.append("CMPNY")
                companyList.append(x)
                df_cik = pd.DataFrame([companyList])
                print(df_cik)
                self.sec_gov.process_submissions(df_cik)

        else:
            ticker_details = self.sec_gov.get_ticker_details()
            self.sec_gov.process_submissions(ticker_details)

    def run(self):
        """
        Execute the data ingestion process.
        """

        try:
            task = self.claim_task(TASK_TYPE, AGENT_ID)
            print(task)
            if task is None:
                self.logger.error(f"Error in Task API claim() method")
                return

            if "status" not in task:
                self.logger.error(
                    f"Error in Task API claim() method - key [status] was not found"
                )
                return

            if (
                not task["status"]
                and "error_message" in task
                and task["error_message"] != "No unclaimed task found"
            ):
                self.logger.error(
                    f"Error in Task API claim() method - {task['error_message']}"
                )
                return

            if (
                not task["status"]
                and "error_message" in task
                and task["error_message"] == "No unclaimed task found"
            ):
                self.logger.info(f"Waiting for work - {task['error_message']}")
                return

            task_id = task["id"]
            task_query = task["query"]
            print(task_query)

            self.logger.info(f"Query: {task_query}")

            self.logger.info("============")
            self.logger.info(f"task id {task_id} started.")
            self.logger.info("Input pipeline:")

            input_json = json.loads(task["query"])
            self.sec_gov = EdgarIngestor(input_json)
            self.process_submissions(input_json)

            self.logger.info("============")
            self.logger.info(f"Task id {task_id} completed.")

            task_completed = self.task_completed(
                object_storage_key_for_results=self.sec_gov.s3_key,
                task_id=task_id,
                success=True,
                message=f"Task id {task_id} completed successfully.",
            )

            if "message" in task_completed:
                self.logger.info(task_completed["message"])
            self.logger.info(f"Task id = {task_id} completed.")

        except (KeyboardInterrupt, TypeError, Exception) as e:
            self.task_completed(
                object_storage_key_for_results=None,
                task_id=task_id,
                success=False,
                message=f"Task id = {task_id} completed.",
            )
            self.logger.exception(f"Task failed: {e}")