import os
import time
import boto3
import logging
import watchtower
from flask import Flask
from ..ingestors.s3.s3storage import S3Storage
from .metric import Metric
from .metric import Metric
from .metrics import Metrics
from datetime import datetime, timezone
from .constants import LogMetricsConstants
from logging.handlers import RotatingFileHandler


class S3RotatingLogFileHandler(RotatingFileHandler):
    """Log handler that supports rotation and upload rotated log to S3 object store"""

    def __init__(self, bucket, key, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0, errors=None):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay, errors)
        self.bucket = bucket
        self.key = key
        self.s3 = S3Storage()
        self.s3.authenticate()

    def doRollover(self):
        timestamp = int(time.time())
        print(f'ROLLOVER - uploading {self.baseFilename} to bucket {self.bucket} object {self.key}_{timestamp}')
        self.s3.upload_file(self.baseFilename, self.bucket, f'{self.key}_{timestamp}.log.txt')

        current_log_file = self.baseFilename
        filename_without_extension = os.path.splitext(os.path.basename(current_log_file))[0]
        new_log_file = os.path.join(LogMetricsConstants.LOG_BACKUP_DIR, os.path.basename(current_log_file))
        os.rename(current_log_file, f'{new_log_file}_{timestamp}.log.txt')

        super().doRollover()


class CloudMultiLogMetrics:
    """CloudMultiLogMetrics is a Logger and Metrics reporter that supports the following features:

        1) separate local logs with info, error levels
        2) s3 info logs with partitions (available and uploaded after rotation)
        3) s3 errors logs with partitions (available and uploaded after rotation)
        4) cloudwatch log stream info logs (real time)
        5) cloudwatch log stream error logs (real time)
        6) cloudwatch metrics (integer counter, real time)


        log_level =
                           LOG_LEVEL_INFO or   (INFO+ level log on a separate log file)
                           LOG_LEVEL_ERROR or  (ERROR+ level log on a separate log file)
                           LOG_LEVEL_ALL       (all of the above)
        log_destination =
                           LOG_DESTINATION_LOCAL or         (logs are written to local vm/pod storage)
                           LOG_DESTINATION_OBJECT_STORE or  (logs are written to s3 storage)
                           LOG_DESTINATION_CLOUD_LOGS or    (logs are written to cloudwatch log storage)
                           LOG_DESTINATION_ALL              (all of the above)
        metrics_destination =
                           METRICS_DESTINATION_LOCAL or     (metrics are written to local vm/pod storage)
                           METRICS_DESTINATION_CLOUD or     (metrics are written to cloudwatch metrics)
                           METRICS_DESTINATION_ALL          (all of the above)

        rotation_byte_size

     """

    def __init__(self,
                 # one needs to have a good understanding of AWS Cloudwatch metrics dimensions
                 namespace: str,  # usually application name
                 buckets: dict,  # strictly structured s3 bucket names
                 dimensions: dict,  # strictly structured dimension name and values
                 log_level: int = LogMetricsConstants.LOG_LEVEL_ALL,
                 log_destination: int = LogMetricsConstants.LOG_DESTINATION_ALL,
                 metrics_destination: int = LogMetricsConstants.METRICS_DESTINATION_ALL,
                 rotation_byte_size: int = 1048576,
                 info_object_storage_key_prefix: str = 'logs/info',
                 error_object_storage_key_prefix: str = 'logs/error',
                 flask_app: Flask = None):

        self.root_logger = logging.getLogger()
        self.root_logger.setLevel(logging.INFO)

        self.root_logger.info("Cloud MultiLog initialization is in progress.")

        dimension_1_name = dimensions['name_1']  # usually job name such as topic_id
        dimension_1_value = dimensions['value_1']  # topic_id value
        dimension_2_name = dimensions['name_2']  # usually job category such as project_id
        dimension_2_value = dimensions['value_2']  # project_id value
        dimension_3_name = dimensions['name_3']  # usually timestamp
        dimension_3_value = dimensions['value_3']  # timestamp value

        info_log_bucket_name = buckets['logs']
        error_log_bucket_name = buckets['errors']

        self.log_level = log_level
        self.log_destination = log_destination
        self.metrics_destination = metrics_destination

        if not os.path.exists(LogMetricsConstants.LOG_BACKUP_DIR):
            os.makedirs(LogMetricsConstants.LOG_BACKUP_DIR)

        self.info_object_storage_key = f'{info_object_storage_key_prefix}/{namespace}/{dimension_2_value}/{dimension_1_value}_{dimension_3_value}/log_info_{dimension_1_value}'
        self.error_object_storage_key = f'{error_object_storage_key_prefix}/{namespace}/{dimension_2_value}/{dimension_1_value}_{dimension_3_value}/log_error_{dimension_1_value}'

        self.root_logger.info(f"S3 Info Log Path = {self.info_object_storage_key}")
        self.root_logger.info(f"S3 Error Log Path = {self.error_object_storage_key}")

        formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter.converter = time.gmtime

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:

            self.info_log_file_name = f'log_info_{dimension_1_value}_{dimension_3_value}.log.txt'
            self.info_logger = logging.getLogger('info_logger')
            self.info_logger.setLevel(logging.INFO)

            self.root_logger.info(f"S3 Info Log key = {self.info_log_file_name}")
            self.root_logger.info(f"Setting up S3RotatingLogFileHandler for S3 Info Log")

            if log_destination & LogMetricsConstants.LOG_DESTINATION_OBJECT_STORE:
                self.info_handler = S3RotatingLogFileHandler(info_log_bucket_name, self.info_object_storage_key,
                                                             self.info_log_file_name, maxBytes=rotation_byte_size,
                                                             backupCount=1024)
                self.info_logger.addHandler(self.info_handler)
                self.info_handler.setFormatter(formatter)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:

            self.error_log_file_name = f'log_error_{dimension_1_value}_{dimension_3_value}.log.txt'
            self.error_logger = logging.getLogger('error_logger')
            self.error_logger.setLevel(logging.ERROR)

            self.root_logger.info(f"S3 Error Log key = {self.error_log_file_name}")
            self.root_logger.info(f"Setting up S3RotatingLogFileHandler for S3 Error Log")

            if self.log_destination & LogMetricsConstants.LOG_DESTINATION_OBJECT_STORE:
                self.error_handler = S3RotatingLogFileHandler(error_log_bucket_name, self.error_object_storage_key,
                                                              self.error_log_file_name, maxBytes=rotation_byte_size,
                                                              backupCount=1024)
                self.error_logger.addHandler(self.error_handler)
                self.error_handler.setFormatter(formatter)

        if self.log_destination & LogMetricsConstants.LOG_DESTINATION_OBJECT_STORE or self.log_destination & LogMetricsConstants.LOG_DESTINATION_CLOUD_LOGS or self.metrics_destination & LogMetricsConstants.METRICS_DESTINATION_CLOUD:

            # initialize s3 buckets

            self.s3 = S3Storage()
            if not self.s3.authenticate():
                raise Exception('Environment variables AWS_ACCESS_KEY, AWS_SECRET_KEY and AWS_REGION are missing')

            region_name_env: str = os.getenv('AWS_REGION')
            if not region_name_env:
                raise Exception('Environment variable AWS_REGION is missing')

            access_key = os.getenv("AWS_ACCESS_KEY")
            if not access_key:
                raise Exception('Environment variable AWS_ACCESS_KEY is missing')

            secret_key = os.getenv("AWS_SECRET_KEY")
            if not secret_key:
                raise Exception('Environment variable AWS_SECRET_KEY is missing')

            self.root_logger.info(f'Initializing boto3 client for logs with in region {region_name_env}')
            self.root_logger.info(f'Initializing boto3 client for cloudwatch with in region {region_name_env}')

            # initialize cloudwatch logs metrics

            boto3_logs_client = boto3.client("logs", region_name=region_name_env, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            boto3_watch_client = boto3.client("cloudwatch", region_name=region_name_env, aws_access_key_id=access_key, aws_secret_access_key=secret_key)

            if log_destination & LogMetricsConstants.LOG_DESTINATION_CLOUD_LOGS:

                if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:

                    self.root_logger.info(f"Setting up watchtower CloudWatchLogHandler for info")

                    watchtower_handler = watchtower.CloudWatchLogHandler(
                        log_group_name=f'{namespace}_info_log',
                        log_stream_name=f'{dimension_1_value}_{dimension_3_value}_{dimension_2_value}',
                        boto3_client=boto3_logs_client)

                    self.info_logger.info(
                        f'Added cloudwatch log handler with log group={namespace}_info_log & log stream={dimension_1_value}_{dimension_3_value}_{dimension_2_value} in {region_name_env} region')

                    self.info_logger.addHandler(watchtower_handler)
                    if flask_app:
                        flask_app.logger.addHandler(watchtower_handler)
                        logging.getLogger("werkzeug").addHandler(watchtower_handler)

                if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:

                    self.root_logger.info(f"Setting up watchtower CloudWatchLogHandler for error")

                    watchtower_handler = watchtower.CloudWatchLogHandler(
                        log_group_name=f'{namespace}_error_log',
                        log_stream_name=f'{dimension_1_value}_{dimension_3_value}_{dimension_2_value}',
                        boto3_client=boto3_logs_client)

                    if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
                        self.info_logger.info(
                            f'Added cloudwatch log handler with log group={namespace}_error_log & log stream={dimension_1_value}_{dimension_3_value}_{dimension_2_value} in {region_name_env} region')

                    self.error_logger.addHandler(watchtower_handler)
                    if flask_app:
                        flask_app.logger.addHandler(watchtower_handler)
                        logging.getLogger("werkzeug").addHandler(watchtower_handler)

            if self.metrics_destination & LogMetricsConstants.METRICS_DESTINATION_CLOUD:

                # initialize metrics sink

                self.root_logger.info(f'Setting up cloudwatch Metrics(namespace={namespace}, region={region_name_env})')

                # this will call boto 3 initialization
                self.metrics_sink = Metrics(
                    namespace=namespace,
                    dimension_1_name=dimension_1_name,
                    dimension_1_value=dimension_1_value,
                    dimension_2_name=dimension_2_name,
                    dimension_2_value=dimension_2_value,
                    dimension_3_name=dimension_3_name,
                    dimension_3_value=dimension_3_value,
                    boto_client=boto3_watch_client
                )
                self.root_logger.info(f'Cloudwatch Metrics setup is complete.')

        self.root_logger.info("Cloud Multilog initialization is complete.")

    # logger methods

    def info(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.info(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.debug(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.error(msg, *args, **kwargs)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:
            self.error_logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.critical(msg, *args, **kwargs)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:
            self.error_logger.critical(msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.fatal(msg, *args, **kwargs)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:
            self.error_logger.fatal(msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info, **kwargs):

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.exception(msg, *args, exc_info, **kwargs)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR:
            self.error_logger.exception(msg, *args, **kwargs)

    # metrics methods

    def write_metric_value(self, metric_name: str, metric_value: int):

        if self.metrics_destination & LogMetricsConstants.METRICS_DESTINATION_CLOUD:
            self.metrics_sink.write(metric_name, metric_value)

        now_utc = datetime.now(timezone.utc)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.info(
                f'metric report at {now_utc.strftime("%Y-%m-%d %H:%M:%S")} UTC {metric_name}={metric_value}')

    def write_metric(self, metric: Metric):

        if self.metrics_destination & LogMetricsConstants.METRICS_DESTINATION_CLOUD:
            self.metrics_sink.write_metric(metric)

        now_utc = datetime.now(timezone.utc)

        if self.log_level & LogMetricsConstants.LOG_LEVEL_INFO:
            self.info_logger.info(
                f'metric report at {now_utc.strftime("%Y-%m-%d %H:%M:%S")} UTC {metric.name}={metric.value}')

    def finalize(self):

        # process the last log file
        if (self.log_level & LogMetricsConstants.LOG_LEVEL_INFO and
                self.log_destination & LogMetricsConstants.LOG_DESTINATION_OBJECT_STORE):
            self.root_logger.info(">>>>> multi log info rotation called")
            self.info_handler.doRollover()
            self.info_logger.handlers.clear()

        if (self.log_level & LogMetricsConstants.LOG_LEVEL_ERROR and
                self.log_destination & LogMetricsConstants.LOG_DESTINATION_OBJECT_STORE):
            self.error_handler.doRollover()
            self.error_logger.handlers.clear()

# if __name__ == "__main__":
#     logger = CloudMultiLogMetrics(application_name='talkwalker', job_name='project_1_topic_1',
#                                   log_level=LogMetricsConstants.LOG_LEVEL_ALL,
#                                   log_destination=LogMetricsConstants.LOG_DESTINATION_ALL,
#                                   metrics_destination=LogMetricsConstants.METRICS_DESTINATION_ALL,
#                                   rotation_byte_size=2048)
#
#     for i in range(1, 200):
#
#         logger.info('info test log entry')
#         logger.debug('debug test log entry')
#         logger.warning('warning test log entry')
#         logger.error('error test log entry')
#         logger.critical('critical test log entry')
#         logger.fatal('fatal test log entry')
#         try:
#             a = 1 / 0
#         except Exception as e:
#             logger.exception('Uncaught Exception - %s', exc_info=e)
#
#         logger.write_metric_value("talkwalker_records", 10)
#
#         m = Metric("talkwalker_saved_records", 11)
#         logger.write_metric(m)
#
#     logger.finalize()