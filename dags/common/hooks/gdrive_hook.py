import os
import io
from airflow.hooks.base_hook import BaseHook
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as CredentialsUser
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import logging


_MIME_TYPES = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.google-apps.spreadsheet',
    '': 'application/octet-stream'}


def _gdrive_path_split(path):
    i = path.find('/')
    return [path[:i], path[i + 1:]]


class GDriveHook(BaseHook):
    """

    This hook is meant to be used in a same fashion as the ftp, sftp, ftps hooks. This is why it defines
    the same methods as them. This link to the google cloud platfrom's doc will help further development:
    https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/drive_v3.files.html
    """

    def __init__(self, gcp_conn_id='gcp_default'):
        super(GDriveHook, self).__init__(self)
        self.gcp_conn_id = gcp_conn_id
        self.conn = None

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
                self.conn = build('drive', 'v3', credentials=creds, cache_discovery=False)
            finally:
                logger.setLevel(logging_level)
        return self.conn

    def retrieve_file(self, remote_full_path, local_full_path_or_buffer):
        # assert False, 'export sheets and docs'
        service = self.get_conn()
        folder_id, file_name = _gdrive_path_split(remote_full_path)
        file = self.get_folder_items(folder_id, file_name)
        logger = logging.getLogger()
        logging_level = logger.getEffectiveLevel()
        logger.setLevel(logging.ERROR)
        try:
            if file and len(file) > 0:
                file_id = file[0]['id']
                request = service.files().get_media(fileId=file_id)
                with io.FileIO(local_full_path_or_buffer, mode='w') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
        finally:
            logger.setLevel(logging_level)

    def retrieve_media_file(self, remote_full_path, local_full_path_or_buffer, requested_mime_type):
        # assert False, 'export sheets and docs'
        service = self.get_conn()
        folder_id, file_name = _gdrive_path_split(remote_full_path)
        file = self.get_folder_items(folder_id, file_name)
        logger = logging.getLogger()
        logging_level = logger.getEffectiveLevel()
        logger.setLevel(logging.ERROR)
        try:
            if file and len(file) > 0:
                file_id = file[0]['id']
                request = service.files().export_media(fileId=file_id, mimeType=requested_mime_type)
                with io.FileIO(local_full_path_or_buffer, mode='w') as f:
                    downloader = MediaIoBaseDownload(f, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
        finally:
            logger.setLevel(logging_level)

    def store_file(self, remote_full_path, local_full_path_or_buffer, overwrite_target=True):
        service = self.get_conn()
        folder_id, file_name = _gdrive_path_split(remote_full_path)
        file = self.get_folder_items(folder_id, file_name)
        ext = os.path.splitext(file_name)[1].lower()
        mime_type = _MIME_TYPES[''] if len(ext) == 0 or ext not in _MIME_TYPES.keys() else _MIME_TYPES[ext]
        logger = logging.getLogger()
        logging_level = logger.getEffectiveLevel()
        logger.setLevel(logging.ERROR)
        try:
            if file and len(file) > 0:
                if overwrite_target:
                    service.files().update(
                        fileId=file[0]['id'],
                        body={
                            'name': file_name,
                            'mimeType': mime_type
                        },
                        media_body=MediaFileUpload(
                            local_full_path_or_buffer,
                            resumable=True),
                        supportsAllDrives='true',
                        fields='id').execute()
                else:
                    self.log.info('Skipping {}, because the file already exists and overwrite_target=True'
                                  .format(remote_full_path))
            else:
                service.files().create(
                    body={
                        'name': file_name,
                        'mimeType': mime_type,
                        'parents': [folder_id]
                    },
                    media_body=MediaFileUpload(
                        local_full_path_or_buffer,
                        resumable=False),
                    supportsAllDrives='true',
                    fields='id').execute()
        finally:
            logger.setLevel(logging_level)

    def get_folder_items(self, folder_id, file_name=None):
        service = self.get_conn()
        logger = logging.getLogger()
        logging_level = logger.getEffectiveLevel()
        logger.setLevel(logging.ERROR)
        try:
            results = service.files().list(
                pageSize=100,
                fields='files(id, name, mimeType, parents)',
                q="'{}' in parents and trashed=false{}".format(
                    folder_id,
                    '' if file_name is None else " and name='{}'".format(file_name)),
                # orderBy='name',
                # corpora='teamDrive',
                includeItemsFromAllDrives='true',
                supportsAllDrives='true',
            ).execute()
            return results.get('files', [])
        finally:
            logger.setLevel(logging_level)

    def list_directory(self, path):
        items = self.get_folder_items(path)
        return [i['name'] for i in items if i['mimeType'] != 'application/vnd.google-apps.folder']

    def delete_file(self, file_id):
        """
        CAREFUL with this! Deletes the item and children for good
        """
        service = self.get_conn()
        service.files().delete(fileId=file_id).execute()

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
