import pendulum

from airflow.contrib.hooks.ftp_hook import FTPSHook, FTPHook
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import dateutil.parser
import re
import json


class FTPSearchOperator(BaseOperator):
    """
    FTPOperator for transferring files from remote host to local with attribute search.
    This operator uses ftp_hook for file transfer.
    :type ftp_hook: :class:`SSHHook`
    :param ftp_conn_id: connection id from airflow Connections.
    :type ftp_conn_id: str
    :param local_filepath: local file path to write files. (templated)
    :type local_filepath: str
    :param remote_filepath: remote file path to search. (templated)
    :type remote_filepath: str
    :param search_expr: regex filename search (templated)
    :type search_expr: str
    :param min_date: minimum last modified date (templated)
    :param ftp_conn_type: connection type.  ftp, ftps, sftp
    :type ftp_conn_type: str
    :param max_date: max last modified date (templated)
    """
    template_fields = ('local_filepath', 'remote_filepath', 'search_expr', 'ftp_conn_id', 'ftp_conn_type', 'min_date', 'max_date')
    ui_color = '#bbd2f7'

    @apply_defaults
    def __init__(self,
                 ftp_conn_id=None,
                 ftp_conn_type='ftp',
                 local_filepath="/tmp/",
                 remote_filepath="/",
                 search_expr='*',
                 min_date=None,
                 max_date=None,
                 *args,
                 **kwargs):
        super(FTPSearchOperator, self).__init__(*args, **kwargs)
        self.ftp_hook = None
        self.ftp_conn_id = ftp_conn_id
        self.ftp_conn_type = ftp_conn_type
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.search_expr = search_expr
        self.min_date = min_date
        self.downloaded_files = list()
        self.max_date = max_date

    @staticmethod
    def _get_date_param(param):
        if not param:
            return pendulum.create(1900, 1, 1)
        if isinstance(param, pendulum.datetime):
            return param
        elif isinstance(param, str):
            return pendulum.parse(param)

    def execute(self, context):
        min_date = FTPSearchOperator._get_date_param(self.min_date or '1900-01-01')
        max_date = FTPSearchOperator._get_date_param(self.max_date or '2999-12-31')
        self.log.info("Using ftp connection: %s", self.ftp_conn_id)
        self.log.info("min date: %s ", min_date.to_datetime_string())
        self.log.info("max date: %s ", max_date.to_datetime_string())
        if self.ftp_conn_type == 'sftp':
            self.ftp_hook = SFTPHook(ftp_conn_id=self.ftp_conn_id)
        elif self.ftp_conn_type == 'ftps':
            self.ftp_hook = FTPSHook(ftp_conn_id=self.ftp_conn_id)
        else:
            self.ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

        self.log.info("ftp connection info: %s ", self.ftp_hook.get_connection(self.ftp_conn_id).port)

        self.log.info("Getting directory listing for %s", self.remote_filepath)
        file_list = self.get_file_list(
            ftp_hook=self.ftp_hook,
            remote_filepath=self.remote_filepath,
            search_expr=self.search_expr,
            min_date=min_date,
            max_date=max_date)

        if file_list is not None:
            self.download_files(self.ftp_hook, file_list)
        else:
            self.log.info("No files found matching filters in %s", self.remote_filepath)

        return self.downloaded_files

    def file_list_filter(self, files, search_expr, min_date, max_date):
        self.log.info("file info: " + json.dumps(files, indent=4))
        return [k for k, v in files.items() if v.get('type') == 'file'
                and (search_expr is None or re.search(search_expr, k))
                and (min_date is None or dateutil.parser.parse(v.get('modify', v.get('modified'))) > min_date)
                and (max_date is None or dateutil.parser.parse(v.get('modify', v.get('modified'))) <= max_date)]

    def get_file_list(self, ftp_hook, remote_filepath, search_expr, min_date, max_date):
        file_list = ftp_hook.describe_directory(remote_filepath)
        if not bool(file_list):
            return None
        else:
            return self.file_list_filter(file_list, search_expr, min_date, max_date)

    def download_files(self, ftp_hook, file_list):
        for filename in file_list:
            full_remote_path = self.remote_filepath + '/' + filename
            full_local_path = self.local_filepath + '/' + filename
            file_msg = "from {0} to {1}".format(full_remote_path, full_local_path)
            self.log.info("Starting to transfer %s", file_msg)
            ftp_hook.retrieve_file(full_remote_path, full_local_path)
            self.downloaded_files.append(full_local_path)
