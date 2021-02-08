from os import path as op
from airflow.hooks.S3_hook import S3Hook
from urllib.parse import urlparse


def _s3_path_split(path):
    p = urlparse(path)
    return [p.netloc, p.path[1:]]


class S3Hook(S3Hook):
    """

    This is just a wrapper of the existing s3hook. It is meant to be used in a similar manner as ftp, sftp, ftps hooks.

    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def retrieve_file(self, remote_full_path, local_full_path_or_buffer):
        bucket, key = _s3_path_split(remote_full_path)
        obj = self.get_key(key=key, bucket_name=bucket)
        with open(local_full_path_or_buffer, 'wb') as f:
            obj.download_fileobj(f)

    def store_file(self, remote_full_path, local_full_path_or_buffer):
        bucket, key = _s3_path_split(remote_full_path)
        self.load_file(filename=local_full_path_or_buffer, key=key, bucket_name=bucket, replace=True)

    def list_directory(self, path):
        bucket, prefix = _s3_path_split(path)
        return [op.basename(k) for k in self.list_keys(bucket_name=bucket, prefix=prefix)
                if k[len(prefix)+1:].find('/') == -1]

    def delete_file(self, path):
        bucket, key = _s3_path_split(path)
        self.delete_objects(bucket, [key])

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
