from re import search as re_search
from os import path as op
from os import remove as os_remove
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


_CONN_TYPES = ['local',  # local file system inside the airflow worker
               'sftp',  # SSH File Transfer Protocol
               'ftps',  # FTP over SSL
               'ftp',  # unencrypted FTP - DO NOT USE
               's3',  # aws S3 object storage
               'gdrive',  # google drive
               'gcs',  # google cloud storage
               ]

_NO_FILES_OUTCOME = ['fail',
                     'succeed',
                     'skip']

_TYPES_MAP = {
    'aws': 's3',
    'google_cloud_platform': 'gdrive'}


def _set_conn_type(conn_type, conn, attr):
    if conn is None:
        final_type = 'local'
    elif conn_type is None:
        final_type = BaseHook.get_connection(conn).conn_type
        if final_type in _TYPES_MAP.keys():
            final_type = _TYPES_MAP[final_type]
    else:
        final_type = conn_type.lower()
    assert final_type != 'ftp', 'Avoid using ftp type for {} because it is not encrypted'.format(attr)
    assert final_type in _CONN_TYPES, '{} must be in {}; got "{}" instead'.format(attr, _CONN_TYPES, final_type)
    return final_type


class CopyOperator(BaseOperator):
    """

    :param source_conn_id: source connextion id
    :type source_conn_id: str
    :param source_type: source type. It can be one of: 'local', 'sftp', 'ftps', 'ftp', 's3', 'gdrive', 'gcs'
        If not set, it will get the type from the source connection type.
    :type source_type: str
    :param source_dir: source directory. It is used with source_dir_search_regex to match pattern in the immediate
        children files of the folder
    :type source_dir: str
    :param source_filepath: Used for single file transfer. (path + file name)
    :type source_filepath: str
    :param source_dir_search_regex: It is used to match pattern in the immediate children files of the source_dir
    :type source_file_xcom_task_id: str
    :param source_file_xcom_task_id: the id of the task that has xcom list of files (path + file name)
    :type source_file_xcom_task_id: str
    :param target_conn_id: target connection id
    :type target_conn_id: str
    :param target_conn_id: target type. It can be one of: 'local', 'sftp', 'ftps', 'ftp', 's3', 'gdrive', 'gcs'
        If not set, it will get the type from the target connection type.
    :type target_conn_id: str
    :param target_dir: It is used with either source_file_xcom_task_id or source_dir_search_regex and source_dir,
        where is a possibility of multi-file transfer.
    :type target_dir: str
    :param target_filepath: Used for single file transfer. (path + file name)
    :type target_filepath: str
    :param no_files_outcome: It can be one of: 'fail', 'succeed', 'skip'. Default is 'fail'
        What will be the exit status if no files are copied
    :type no_files_outcome: str

    Sample paths:
    s3: s3://bucket.name/key_folder/key.json
    gcs: gs://bucket.name/key_folder/key.json
    local: /etl/test.json
    gdrive: 13Gb7hcN-RK4ozZySH0M28I1236w8PaW8/test.json
        (the_folder_id/file_name)
        Folders for gdrive are refered to by ID, but files by name. There can be multiple files with the same name in
        google drive. In that case, only the randomly picked first file gets updated / deleted
    """

    template_fields = ('source_dir_search_regex', 'source_filepath', 'target_filepath', 'source_dir', 'target_dir')
    ui_color = '#ffeefa'

    @apply_defaults
    def __init__(self,
                 source_conn_id=None,
                 source_type=None,
                 source_dir=None,
                 source_filepath=None,
                 source_dir_search_regex=None,
                 source_file_xcom_task_id=None,
                 target_conn_id=None,
                 target_type=None,
                 target_dir=None,
                 target_filepath=None,
                 no_files_outcome='fail',
                 overwrite_file=True,
                 *args,
                 **kwargs):
        super(CopyOperator, self).__init__(*args, **kwargs)
        self.source_conn_id = source_conn_id
        self.source_type = _set_conn_type(source_type, source_conn_id, 'source_type')
        self.source_dir = source_dir
        self.source_filepath = source_filepath
        self.source_dir_search_regex = source_dir_search_regex
        self.source_file_xcom_task_id = source_file_xcom_task_id
        self.target_conn_id = target_conn_id
        self.target_type = _set_conn_type(target_type, target_conn_id, 'target_type')
        self.target_dir = target_dir if target_dir is not None else op.dirname(target_filepath)
        self.target_filepath = target_filepath
        self.no_files_outcome = no_files_outcome
        self.overwrite_file = overwrite_file
        self.copied_files = []
        self.existing_target_files = []
        assert target_dir or target_filepath, "target_dir or target_filepath must be set"
        assert no_files_outcome in _NO_FILES_OUTCOME, \
            f'no_files_outcome must be in {_NO_FILES_OUTCOME}; got "{no_files_outcome}" instead'

    @staticmethod
    def _create_hook(conn_id, conn_type):
        if conn_type == 'ftps':
            from .hooks._ftp_hook import FTPSHook
            return FTPSHook(conn_id)
        elif conn_type == 'ftp':
            from .hooks._ftp_hook import FTPHook
            return FTPHook(conn_id)
        elif conn_type == 'sftp':
            from airflow.contrib.hooks.sftp_hook import SFTPHook
            return SFTPHook(conn_id)
        elif conn_type == 's3':
            from .hooks._s3_hook import S3Hook
            return S3Hook(conn_id)
        elif conn_type == 'gdrive':
            from .hooks._gdrive_hook import GDriveHook
            return GDriveHook(conn_id)
        elif conn_type == 'gcs':
            from .hooks._gcs_hook import GCSHook
            return GCSHook(conn_id)
        elif conn_type == 'local':
            from .hooks._local_hook import LocalHook
            return LocalHook(conn_id)

    def _copy_file(self, source_path, target_path, src_hook, trg_hook):
        try:
            if not self.overwrite_file and op.basename(source_path) in self.existing_target_files:
                self.log.info('Skipping file {} {}, because it exists on target {} {}'
                              .format(self.source_type, source_path, self.target_type, op.dirname(target_path)))
                return
            else:
                self.log.info('Copying file from {} {} to {} {}'
                              .format(self.source_type, source_path, self.target_type, target_path))
            if self.source_type == 'local':
                if self.target_type == 'local':
                    import shutil
                    shutil.copyfile(source_path, target_path)
                else:
                    trg_hook.store_file(target_path, source_path)
            elif self.target_type == 'local':
                src_hook.retrieve_file(source_path, target_path)
            else:
                from tempfile import NamedTemporaryFile
                with NamedTemporaryFile("w") as tf:
                    src_hook.retrieve_file(source_path, tf.name)
                    trg_hook.store_file(target_path, tf.name)
            self.copied_files.append(target_path)
        except Exception as e:
            if self.target_type == 'local' and op.getsize(target_path) == 0:
                os_remove(target_path)
            if self.no_files_outcome == 'skip':
                self.log.info(e)
            else:
                raise e

    def execute(self, context):
        with self._create_hook(self.source_conn_id, self.source_type) as src_hook, \
                self._create_hook(self.target_conn_id, self.target_type) as trg_hook:
            if not self.overwrite_file:
                self.existing_target_files = trg_hook.list_directory(self.target_dir)
            if self.source_file_xcom_task_id is not None:
                assert self.target_dir is not None, "target_dir must be set to retrieve files"
                files = context['task_instance'].xcom_pull(self.source_file_xcom_task_id)
                if self.source_dir_search_regex is not None:
                    files = [f for f in files if re_search(self.source_dir_search_regex, f)]
                for f in files:
                    self._copy_file(f, op.join(self.target_dir, op.basename(f)), src_hook, trg_hook)
            elif self.source_dir_search_regex is not None:
                assert self.source_dir is not None, "source_dir must be set whenever source_dir_search_regex is used"
                assert self.target_dir is not None, "target_dir must be set whenever source_dir_search_regex is used"
                files = [f for f in src_hook.list_directory(self.source_dir)
                         if re_search(self.source_dir_search_regex, f)]
                for f in files:
                    self._copy_file(op.join(self.source_dir, f), op.join(self.target_dir, f), src_hook, trg_hook)
            else:
                assert self.source_filepath is not None, "source_filepath must be set to copy a single file"
                assert self.target_filepath is not None, "target_filepath must be set to copy a single file"
                self._copy_file(self.source_filepath, self.target_filepath, src_hook, trg_hook)
        if len(self.copied_files) == 0:
            if self.no_files_outcome == 'fail':
                from airflow.exceptions import AirflowException
                raise AirflowException('No files were copied and no_files_outcome is set to "fail"')
            elif self.no_files_outcome == 'skip':
                from airflow.exceptions import AirflowSkipException
                self.log.info('No files were copied and no_files_outcome is set to "skip"')
                raise AirflowSkipException
        return self.copied_files
