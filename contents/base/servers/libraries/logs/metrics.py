import boto3
from .metric import Metric


class Metrics:
    """Metrics is a wrapper class for Cloud Metrics Service to report integer metrics"""

    def __init__(self,
                 namespace: str,
                 dimension_1_name: str,
                 dimension_1_value: str,
                 dimension_2_name: str,
                 dimension_2_value: str,
                 dimension_3_name: str,
                 dimension_3_value: str,
                 boto_client):
        self.namespace = namespace
        self.dimension_1_name = dimension_1_name
        self.dimension_1_value = dimension_1_value
        self.dimension_2_name = dimension_2_name
        self.dimension_2_value = dimension_2_value
        self.dimension_3_name = dimension_3_name
        self.dimension_3_value = dimension_3_value

        self.client = boto_client

    def write_metric(self, metric: Metric) -> None:
        self.write(metric.name, metric.value)

    def write(self, metric_name, metric_value) -> None:

        self.client.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': self.dimension_1_name,
                            'Value': self.dimension_1_value
                        },
                        {
                            'Name': self.dimension_2_name,
                            'Value': self.dimension_2_value
                        },
                        {
                            'Name': self.dimension_3_name,
                            'Value': self.dimension_3_value
                        }
                    ],
                    'Value': metric_value,  # The value of the metric
                    'Unit': 'Count'  # The unit of the metric
                }
            ]
        )

        # self.client.put_metric_data(Namespace=self.namespace,
        #                             MetricData=[{"MetricName": metric_name, "Value": metric_value, "Unit": 'Count'}])