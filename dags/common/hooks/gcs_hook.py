import os
import io
from airflow.hooks.base_hook import BaseHook
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as CredentialsUser
from google.cloud import storage
import logging
from urllib.parse import unquote, urlparse


def _gcs_path_split(path):
    p = urlparse(path)
    return [p.netloc, p.path[1:]]


def _gcs_blob_path_split(path):
    p = path.split('/')
    if len(p) == 5:
        return [unquote(p[2]), unquote(p[4])]
    else:
        return []


class GCSHook(BaseHook):
    """

    This hook is meant to be used in a same fashion as the ftp, sftp, ftps hooks. This is why it defines
    the same methods as them.
    """

    def __init__(self, gcp_conn_id='gcp_default'):
        super(GCSHook, self).__init__(self)
        self.gcp_conn_id = gcp_conn_id
        self.conn = None
        self.project_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.gcp_conn_id)
            extra = params.extra_dejson
            logger = logging.getLogger()
            logging_level = logger.getEffectiveLevel()
            logger.setLevel(logging.ERROR)
            try:
                if 'authorized_user_file' in extra['extra__google_cloud_platform__key_path']:
                    creds = CredentialsUser.from_authorized_user_file(extra['extra__google_cloud_platform__key_path'])
                else:
                    creds = Credentials.from_service_account_file(extra['extra__google_cloud_platform__key_path'])
                self.project_id = creds.project_id
                self.conn = storage.Client(credentials=creds, project=self.project_id)
            finally:
                logger.setLevel(logging_level)
        return self.conn

    def retrieve_file(self, remote_full_path, local_full_path_or_buffer):
        with io.FileIO(local_full_path_or_buffer, mode='w') as f:
            client = self.get_conn()
            client.download_blob_to_file(remote_full_path, f)

    def store_file(self, remote_full_path, local_full_path_or_buffer):
        client = self.get_conn()
        bucket_name, prefix = _gcs_path_split(remote_full_path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(prefix)
        blob.upload_from_filename(local_full_path_or_buffer)

    def list_directory(self, path):
        client = self.get_conn()
        bucket, prefix = _gcs_path_split(path)
        files = []
        for blob in client.list_blobs(bucket_or_name=bucket, prefix=prefix):
            blob_bucket, key = _gcs_blob_path_split(blob.path)
            if key[len(prefix)+1:].find('/') == -1:
                files.append(os.path.basename(key))
        return files

    def delete_file(self, path):
        client = self.get_conn()
        bucket_name, prefix = _gcs_path_split(path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(prefix)
        blob.delete()

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
