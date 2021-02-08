import os
from common.hooks._ftp_hook import FTPHook, FTPSHook
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FTPOperation(object):
    PUT = 'put'
    GET = 'get'


class FTPOperator(BaseOperator):
    """
    FTPOperator for transferring files from remote host to local or vice a versa.
    This operator uses ftp_hook for file transfer.

    :param ftp_conn_id: connection id from airflow Connections.
    :type ftp_conn_id: str
    :param local_filepath: local file path to get or put. (templated)
    :type local_filepath: str
    :param remote_filepath: remote file path to get or put. (templated)
    :type remote_filepath: str
    :param operation: specify operation 'get' or 'put', defaults to put
    :type get: bool
    """
    template_fields = ('local_filepath', 'remote_filepath', 'local_dir', 'remote_dir', 'file_list_xcom_location')
    ui_color = '#bbd2f7'

    @apply_defaults
    def __init__(self,
                 ftp_conn_id=None,
                 local_filepath=None,
                 remote_filepath=None,
                 local_dir=None,
                 remote_dir=None,
                 operation=FTPOperation.PUT,
                 file_list_xcom_location=None,
                 *args,
                 **kwargs):
        super(FTPOperator, self).__init__(*args, **kwargs)
        self.ftp_conn_id = ftp_conn_id
        self.local_filepath = local_filepath
        self.remote_filepath = remote_filepath
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.operation = operation
        self.file_list_xcom_location = file_list_xcom_location
        if not (self.operation.lower() == FTPOperation.GET or
                self.operation.lower() == FTPOperation.PUT):
            raise TypeError("unsupported operation value {0}, expected {1} or {2}"
                            .format(self.operation, FTPOperation.GET, FTPOperation.PUT))

    def _create_hook(self):
        """Return connection hook."""
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    def execute(self, context):
        with self._create_hook() as hook:
            if self.file_list_xcom_location is not None:
                if self.operation.lower() == FTPOperation.GET:
                    for f in context['task_instance'].xcom_pull(self.file_list_xcom_location):
                        assert self.local_dir is not None, "local_dir must be set to retrieve files"
                        self.transfer_file(hook, os.path.join(self.local_dir, os.path.basename(f)), f)
                else:
                    for f in context['task_instance'].xcom_pull(self.file_list_xcom_location):
                        assert self.remote_dir is not None, "remote_dir must be set to send files"
                        self.transfer_file(hook, f, os.path.join(self.remote_dir, os.path.basename(f)))
            else:
                assert self.local_filepath is not None, "local_filepath must be set"
                assert self.remote_filepath is not None, "remote_filepath must be set"
                self.transfer_file(hook, self.local_filepath, self.remote_filepath)

    def transfer_file(self, hook, local, remote):
        file_msg = None
        try:
            if self.operation.lower() == FTPOperation.GET:
                file_msg = "from {0} to {1}".format(remote, local)
                self.log.info("Starting to get file %s", file_msg)
                hook.retrieve_file(remote, local)
            else:
                file_msg = "from {0} to {1}".format(local, remote)
                self.log.info("Starting to put file %s", file_msg)
                hook.store_file(remote, local)
        except Exception as e:
            raise AirflowException("Error while transferring {0}, error: {1}".format(file_msg, str(e)))


class FTPSOperator(FTPOperator):
    """
    FTPSOperator for transferring files from remote host to local or vice a versa.
    This operator uses ftps_hook for file transfer.
    """
    def _create_hook(self):
        """Return connection hook."""
        return FTPSHook(ftp_conn_id=self.ftp_conn_id)


class SFTPOperator(FTPOperator):
    """
    SFTPOperator for transferring files from remote host to local or vice a versa.
    This operator uses ftps_hook for file transfer.
    """

    def _create_hook(self):
        """Return connection hook."""
        return SFTPHook(ftp_conn_id=self.ftp_conn_id)
