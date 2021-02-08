import os
import os.path as op
from airflow.models import BaseOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.utils.decorators import apply_defaults


class SFTPToSFTPOperator(BaseOperator):
    """
    This operator enables the transfer of files from a SFTP to one or more SFTP locations
    :param work_path:           Local work path that will be used to keep temporary copy of the file in transmission.
    :type work_path:            str
    :param source_conn_id:      The connection id for the source SFTP.
    :type source_conn_id:       str
    :param source_path:         The path on the source SFTP server.
    :type source_path:          str
    :param target_full_path:    The connections and paths for the target SFTPs.
    :type target_full_path:     List[Tuple2(str,str)]
    :param filter_function:     Function that is called to filter the file name
                                filter_function(file_name:str):bool
    :type filter_function:      function
    :param overwrite_target:    Based on file name, this operator can skip to download and upload a file if it exists
                                on destination FTP already.
    :type overwrite_target:     bool
    """


    @apply_defaults
    def __init__(self,
                 work_path,
                 source_conn_id,
                 source_path,
                 target_full_path,
                 filter_function=None,
                 overwrite_target=False,
                 *args,
                 **kwargs):
        super(SFTPToSFTPOperator, self).__init__(*args, **kwargs)
        self.work_path = work_path
        self.source_conn_id = source_conn_id
        self.source_path = source_path
        self.target_full_path = target_full_path
        self.filter_function = filter_function
        self.overwrite_target = overwrite_target

    def execute(self, context):
        source_hook = SFTPHook(ftp_conn_id=self.source_conn_id)
        source_files = source_hook.list_directory(self.source_path)
        for target in self.target_full_path:
            target_connection = target[0]
            target_path = target[1]
            self.log.info(f"Beginning transfer to SFTP site {target_connection} and directory {target_path}")
            target_hook = SFTPHook(ftp_conn_id=target_connection)
            target_files = target_hook.list_directory(target_path)

            for file in source_files:
                if self.filter_function is None or self.filter_function(file):
                    if self.overwrite_target is True or file not in target_files:
                        source_hook.retrieve_file(op.join(self.source_path, file), op.join(self.work_path, file))
                        self.log.info("Downloaded the file %s from the source SFTP", file)
                        try:
                            target_hook.store_file(op.join(target_path, file), op.join(self.work_path, file))
                            self.log.info("Uploaded the file %s to the destination SFTP", file)
                        finally:
                            os.remove(os.path.join(self.work_path, file))