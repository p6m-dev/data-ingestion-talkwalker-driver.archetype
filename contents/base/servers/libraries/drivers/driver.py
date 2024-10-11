import logging
import requests


class Driver:
    def __init__(self):
        logging.basicConfig(
            format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO
        )

        #todo: pass this an env var
        #self.task_queue_url = "http://54.189.63.248:8000/tasks"
        self.task_queue_url = "http://job-server.job.svc.cluster.local/tasks"

        #todo: take ot metadata service call
        # self.meta_api_url = "http://54.188.175.65:8000"

        # self.logger = logging.getLogger()

    def _request(self, method, endpoint, params):
        url = f"{self.task_queue_url}{endpoint}"
        response = requests.request(method, url, params=params)
        data = response.json()
        if data["status"] == False:
            return data

        if data.get("data"):
            return {"status": True, **data.get("data")}
        return data

    def put_task(self, task_type, query):
        endpoint = "/"
        params = {"task_type": task_type, "query": query}
        return self._request("PUT", endpoint, params)

    def claim_task(self, task_type, agent_id):
        endpoint = "/claim"
        params = {"task_type": task_type, "agent_id": agent_id}
        return self._request("POST", endpoint, params)

    def task_completed(self, object_storage_key_for_results, task_id, success, message):
        endpoint = "/complete"
        params = {
            "object_storage_key_for_results": object_storage_key_for_results,
            "task_id": task_id,
            "success": success,
            "message": message,
        }
        return self._request("PUT", endpoint, params)

#     # meta_api methods
#     def get_original_documents_range(self):
#         endpoint_url = f"{self.meta_api_url}/get_original_documents_range/"
#         response = requests.get(endpoint_url)
#         return response.json()
#
#     def get_text_documents_range(self):
#         endpoint_url = f"{self.meta_api_url}/get_text_documents_range/"
#         response = requests.get(endpoint_url)
#         return response.json()

#     def put_original_document_metadata(
#         self, task_queue_id, task_type, task_agent, original_document_uri, metadata
#     ):
#         endpoint_url = f"{self.meta_api_url}/original-document/metadata/"
#         params = {
#             "task_queue_id": task_queue_id,
#             "task_type": task_type,
#             "task_agent": task_agent,
#             "original_document_uri": original_document_uri,
#         }
#         json_data = {"metadata": metadata}
#         response = requests.post(endpoint_url, params=params, json=json_data)
#         # print(response)
#         return response.json()

#     def put_text_document_metadata(
#         self,
#         task_queue_id,
#         task_type,
#         task_agent,
#         original_document_uri,
#         text_document_uri,
#         file_metadata,
#         page_metadata,
#     ):
#         endpoint_url = f"{self.meta_api_url}/text-document/metadata/"
#         params = {
#             "task_queue_id": task_queue_id,
#             "task_type": task_type,
#             "task_agent": task_agent,
#             "original_document_uri": original_document_uri,
#             "text_document_uri": text_document_uri,
#         }
#         json_data = {"file_metadata": file_metadata, "page_metadata": page_metadata}
#         response = requests.post(endpoint_url, params=params, json=json_data)
#         return response.json()
#
#     def get_original_document_metadata(self, doc_id):
#         endpoint_url = f"{self.meta_api_url}/original-document/metadata/{doc_id}"
#         response = requests.get(endpoint_url)
#         return response.json()
#
#     def get_text_document_metadata(self, doc_id):
#         endpoint_url = f"{self.meta_api_url}/text-document/metadata/{doc_id}"
#         response = requests.get(endpoint_url)
#         return response.json()