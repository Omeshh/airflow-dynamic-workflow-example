from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils import apply_defaults
import ntpath


class LocalToS3Operator(BaseOperator):
    """
       This operator enables the transferring a list of local files to a specific location in Amazon S3
       :param s3_conn_id:      The s3 connnection id. The name or identifier for
                               establishing a connection to S3
       :type s3_conn_id:       string
       :param s3_bucket:       The targeted s3 bucket. This is the S3 bucket
                               to where the file is uploaded.
       :type s3_bucket:        string

       :param s3_prefix:          Prefix to prepend to uploaded files within bucket.
       :type s3_prefix:           string

       :param file_list_xcom_location:  Location to pull list of files to upload.
       :type file_list_xcom_location:   string
       """

    template_fields = {"file_list_xcom_location", "s3_bucket", "s3_prefix"}

    @apply_defaults
    def __init__(self,
                 s3_conn_id='aws_default',
                 s3_bucket=None,
                 s3_prefix='',
                 file_list_xcom_location=None,
                 *args,
                 **kwargs):
        super(LocalToS3Operator, self).__init__(*args, **kwargs)
        self.file_list_xcom_location = file_list_xcom_location
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    def execute(self, context):
        sent_files = list()
        if self.file_list_xcom_location is not None:
            file_list = context['task_instance'].xcom_pull(self.file_list_xcom_location)
        else:
            file_list = list()
        for file in file_list:
            self.log.info("Working on file: %s", str(file))
            if not ntpath.isfile(file):
                raise FileNotFoundError
            basename = ntpath.basename(file)
            s3_key = self.get_s3_key(self.s3_prefix, basename)
            self.log.info("Sending %s to S3 bucket %s, key %s", file, self.s3_bucket, s3_key)
            self.s3_hook.load_file(file, s3_key, self.s3_bucket, replace=True)
            sent_files.append(s3_key)

        context['task_instance'].xcom_push(key='s3_files', value=sent_files)

    @staticmethod
    def get_s3_key(prefix, filename):
        s3_key = urlparse(prefix + "/" + filename)
        return s3_key.path.lstrip('/')
