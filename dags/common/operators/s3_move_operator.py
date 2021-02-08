import os

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3MoveOperator(BaseOperator):
    template_fields = ('source_bucket_key', 'dest_bucket_key',
                       'source_bucket_name', 'dest_bucket_name')

    @apply_defaults
    def __init__(
            self,
            source_bucket_name,
            file_list_xcom_location=None,
            source_bucket_key=None,
            dest_bucket_key=None,
            dest_bucket_name=None,
            source_version_id=None,
            dest_prefix=None,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(S3MoveOperator, self).__init__(*args, **kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name or source_bucket_name
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.file_list_xcom_location = file_list_xcom_location
        self.dest_prefix = dest_prefix

        assert (self.file_list_xcom_location and dest_prefix) or (source_bucket_key and dest_bucket_key), "Invalid parameters.  File list or source/destination keys must be set"

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        if self.file_list_xcom_location is not None:
            if isinstance(self.file_list_xcom_location, tuple):
                file_list = context['task_instance'].xcom_pull(task_ids=self.file_list_xcom_location[0], key=self.file_list_xcom_location[1])
            else:
                file_list = context['task_instance'].xcom_pull(self.file_list_xcom_location)
            if file_list is None:
                file_list = list()
            for source_key in file_list:
                dest_key = os.path.join(self.dest_prefix, os.path.basename(source_key))
                self.log.info(f"Moving {source_key} to {dest_key}.")
                self.move(s3_hook, source_key, dest_key)
        else:
            self.move(s3_hook, self.source_bucket_key, self.dest_bucket_key)

    def move(self, s3_hook, source_key, dest_key):
        s3_hook.copy_object(source_key, dest_key,
                            self.source_bucket_name, self.dest_bucket_name,
                            self.source_version_id)
        s3_hook.delete_objects(self.source_bucket_name, source_key)
