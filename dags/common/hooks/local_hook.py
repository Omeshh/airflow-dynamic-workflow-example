import os
from airflow.hooks.base_hook import BaseHook


class LocalHook(BaseHook):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @staticmethod
    def list_directory(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    def get_conn(self):
        pass

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
