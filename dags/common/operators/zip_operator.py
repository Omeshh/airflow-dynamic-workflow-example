from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from zipfile import ZipFile
import os
import os.path as op
import logging


class ZipOperator(BaseOperator):
    """
    An operator which takes in a path to a file and zips the contents to a location you define.
    :param zip_filepath: Full path to where you want to save the Zip file
    :type zip_filepath: string
    :param filepath: Full path to the file you want to Zip
    :type filepath: string
    :filename_ends_with: Filter files with name ending with string
    :type filename_ends_with: string
    """
    template_fields = ('filepath', 'zip_filepath')
    ui_color = '#e09eca'

    @apply_defaults
    def __init__(self,
                 zip_filepath,
                 filepath,
                 filename_ends_with=None,
                 *args, **kwargs):
        super(ZipOperator, self).__init__(*args, **kwargs)
        self.zip_filepath = zip_filepath
        self.filepath = filepath
        self.filename_ends_with = filename_ends_with

    def execute(self, context):
        if op.isfile(self.filepath):
            dir_path = op.dirname(op.abspath(self.filepath))
        else:
            dir_path = self.filepath

        os.chdir(dir_path)
        logging.info("Current Working Directory: {0}".format(str(os.getcwd())))

        with ZipFile(self.zip_filepath, 'w') as zip_file:
            logging.info("Created zip file: {0}".format(self.zip_filepath))
            if op.isfile(self.filepath):
                logging.info("Writing '{0}' to zip file".format(op.basename(self.filepath)))
                zip_file.write(op.basename(self.filepath))
            else:  # is folder
                for filename in os.listdir(self.filepath):
                    if self.filename_ends_with is None or filename.endswith(self.filename_ends_with):
                        logging.info("Writing '{0}' to zip file".format(filename))
                        zip_file.write(filename)

            zip_file.close()

        logging.info("Finished writing to zip file")


class UnzipOperator(BaseOperator):
    """
    An operator which takes in a path to a zip file and unzips the contents to a location you define.
    :param zip_filepath: Full path to the zip file you want to Unzip
    :type zip_filepath: string
    :param unzip_path: Full path to where you want to save the contents of the Zip file you're Unzipping
    :type unzip_path: string
    :param file_list_xcom_location: Xcom path to pull list of files.  Use this when unzipping multiple files from a previous operator
    :type file_list_xcom_location: string
    """
    template_fields = ('zip_filepath', 'unzip_path', 'file_list_xcom_location')
    ui_color = '#a6ff4d'

    @apply_defaults
    def __init__(
            self,
            unzip_path=None,
            zip_filepath=None,
            file_list_xcom_location=None,
            *args, **kwargs):
        super(UnzipOperator, self).__init__(*args, **kwargs)

        self.zip_filepath = zip_filepath
        self.unzip_path = unzip_path
        self.file_list_xcom_location = file_list_xcom_location

    def execute(self, context):
        unzipped_files = list()
        if bool(self.zip_filepath):
            zip_file_list = list(self.zip_filepath)
        else:
            if bool(self.file_list_xcom_location):
                zip_file_list = context['task_instance'].xcom_pull(self.file_list_xcom_location)
            else:
                zip_file_list = list()
        for file in zip_file_list:
            with ZipFile(file, 'r') as zip_file:
                logging.info("Extracting all the contents of '{0}' to '{1}'".format(self.zip_filepath, self.unzip_path))
                unzipped_files.append(zip_file.extractall(self.unzip_path))
                zip_file.close()

        logging.info("Finished unzipping zip file")
        return unzipped_files
