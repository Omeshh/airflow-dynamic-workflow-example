from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils import apply_defaults
import ntpath


class S3ToLocalOperator(BaseOperator):
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
                 dest_directory='',
                 file_list_xcom_location=None,
                 *args,
                 **kwargs):
        super(S3ToLocalOperator, self).__init__(*args, **kwargs)
        self.file_list_xcom_location = file_list_xcom_location
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.dest_directory = dest_directory
        self.s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    @staticmethod
    def get_filename_from_key(key):
        return key.split('/')[-1]

    def download(self, s3_key):
        self.log.info("Getting %s from S3 bucket %s, to %s", s3_key, self.s3_bucket, self.dest_directory)
        obj = self.s3_hook.get_key(s3_key, self.s3_bucket)
        filename = self.get_filename_from_key(s3_key)
        if filename:
            full_filename = self.dest_directory + '/' + filename
            with open(full_filename, 'wb') as f:
                f.writelines(obj.get()['Body'])
            return full_filename
        else:
            return None

    def execute(self, context):
        sent_files = list()
        sent_keys=list()
        if self.s3_prefix is not None:
            file_list = self.s3_hook.list_keys(self.s3_bucket, self.s3_prefix, '/') or list()
        else:
            file_list = list()
        if file_list.__len__() == 0:
            print("File list is empty")
        for file in file_list:
            self.log.info("Working on file: %s", str(file))
            file_download = self.download(file)
            if file_download:
                self.log.info("Downloaded file %s", str(file_download))
                sent_files.append(file_download)
                sent_keys.append(file)
        context['ti'].xcom_push(key='downloaded_keys', value=sent_keys)
        return sent_files

    @staticmethod
    def get_s3_key(prefix, filename):
        s3_key = urlparse(prefix + "/" + filename)
        return s3_key.path.lstrip('/')
