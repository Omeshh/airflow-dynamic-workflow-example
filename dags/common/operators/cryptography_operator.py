from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import gnupg
import os
from re import search as re_search


class CryptographyOperation(object):
    ENCRYPT = 'encrypt'
    DECRYPT = 'decrypt'


# noinspection PyUnresolvedReferences
class CryptographyOperator(BaseOperator):
    """
    CryptographyOperator for encrypting or decrypting file using GPG.
    This operator uses base_hook to get the encryption key file name and passphrase.

    :param crypto_conn_id: connection id from airflow Connections.
    :type crypto_conn_id: str
    :param src_filepath: source file path to encrypt or decrypt. (templated)
    :type src_filepath: str
    :param dest_filepath: destination file path to encrypt or decrypt. (templated)
    :type dest_filepath: str
    :param recipients: encrypt file for recipients.
    :type recipients: sequence (list, tuple, set)
    :param operation: specify operation 'encrypt' or 'decrypt', defaults to decrypt
    :type operation: str
    :param remove_unencrypted: specify whether to delete the original unencrypted file after encrypting
    :type remove_unencrypted: bool
    :param remove_decrypted: specify whether to delete the original encrypted file after decrypting
    :type remove_decrypted: bool
    :param file_list_xcom_location: specify the variable that holds list of files to be encrypted/decrypted
    :type file_list_xcom_location: str
    :param output_directory: specify the final location for multiple files being encrypted/decrypted
    :type output_directory: str
    :param file_list_search_regex: use regex to pick specific files from xcom
    :type file_list_search_regex: str
    """
    template_fields = ('src_filepath', 'dest_filepath', 'output_directory', 'file_list_search_regex')
    ui_color = '#bbd2f7'

    @apply_defaults
    def __init__(self,
                 crypto_conn_id,
                 src_filepath=None,
                 dest_filepath=None,
                 recipients=None,
                 operation=CryptographyOperation.DECRYPT,
                 fingerprint=None,
                 remove_unencrypted=False,
                 remove_encrypted=False,
                 file_list_xcom_location=None,
                 output_directory=None,
                 file_list_search_regex=None,
                 *args,
                 **kwargs):
        super(CryptographyOperator, self).__init__(*args, **kwargs)
        self.crypto_conn_id = crypto_conn_id
        self.src_filepath = src_filepath
        self.dest_filepath = dest_filepath
        self.recipients = recipients
        self.operation = operation
        self.fingerprint = fingerprint
        self.remove_unencrypted = remove_unencrypted
        self.remove_encrypted = remove_encrypted
        self.file_list_xcom_location = file_list_xcom_location
        self.output_directory = output_directory
        self.file_list_search_regex = file_list_search_regex
        if not (self.operation.lower() == CryptographyOperation.ENCRYPT or
                self.operation.lower() == CryptographyOperation.DECRYPT):
            raise TypeError("unsupported operation value {0}, expected {1} or {2}"
                            .format(self.operation, CryptographyOperation.ENCRYPT, CryptographyOperation.DECRYPT))

        conn = BaseHook.get_connection(self.crypto_conn_id)
        conn_options = conn.extra_dejson
        self.key_file = conn_options.get('key_file')
        self.gpg_options = conn_options.get('options')
        self._passphrase = conn.password

    def _encrypt(self, src_filepath, dest_filepath):
        """Encrypts the source file using GPG with key_file and passphrase provided in connection."""
        self.log.info("Encrypting file {0} to {1}.".format(src_filepath, dest_filepath))

        gpg = gnupg.GPG(options=self.gpg_options)
        key_data = open(self.key_file, mode='rb').read()
        import_result = gpg.import_keys(key_data)
        self.log.info("Key import results: {0}".format(import_result.results))

        with open(src_filepath, 'rb') as f:
            status = gpg.encrypt_file(f,
                                      passphrase=self._passphrase,
                                      output=dest_filepath,
                                      recipients=self.recipients)
            self.log.info("ok: {0}, status:{1}, stderr: {2}".format(status.ok, status.status, status.stderr))

            if status.ok and self.remove_unencrypted:
                os.remove(src_filepath)

            if not status.ok:
                raise AirflowException("Failed to encrypt file {0}: {1}"
                                       .format(src_filepath, status.stderr))

        self.log.info("Completed file encryption.")

    def _decrypt(self, src_filepath, dest_filepath):
        """Decrypts the source file using GPG with key_file and passphrase provided in connection."""
        self.log.info("Decrypting file {0} to {1}.".format(src_filepath, dest_filepath))

        gpg = gnupg.GPG(options=self.gpg_options)
        key_data = open(self.key_file, mode='rb').read()
        import_result = gpg.import_keys(key_data)
        self.log.info("Key import results: {0}".format(import_result.results))

        with open(src_filepath, 'rb') as f:
            status = gpg.decrypt_file(f,
                                      passphrase=self._passphrase,
                                      output=dest_filepath)
            self.log.info("ok: {0}, status:{1}, stderr: {2}".format(status.ok, status.status, status.stderr))

            if status.ok and self.remove_encrypted:
                os.remove(src_filepath)

            if not status.ok:
                raise AirflowException("Failed to decrypt file {0}: {1}"
                                       .format(src_filepath, status.stderr))

        self.log.info("Completed file decryption.")

    def execute(self, context):
        if self.fingerprint is not None:
            gpg = gnupg.GPG()
            gpg.trust_keys(self.fingerprint, 'TRUST_ULTIMATE')
        output_list = list()
        if self.file_list_xcom_location is not None:
            file_list = context['task_instance'].xcom_pull(self.file_list_xcom_location)
            if self.file_list_search_regex is not None:
                file_list = [f for f in file_list if re_search(self.file_list_search_regex, f)]
            for file in file_list:
                if self.operation.lower() == CryptographyOperation.ENCRYPT:
                    result_file = os.path.join(self.output_directory, os.path.basename(file)) + '.gpg'
                    self._encrypt(file, result_file)
                    output_list.append(result_file)
                else:
                    result_file = os.path.join(self.output_directory, os.path.splitext(os.path.basename(file))[0])
                    self._decrypt(file, result_file)
                    output_list.append(result_file)
        else:
            output_list.append(self.output_directory)
            if self.operation.lower() == CryptographyOperation.ENCRYPT:
                self._encrypt(self.src_filepath, self.dest_filepath)
            else:
                self._decrypt(self.src_filepath, self.dest_filepath)

        return output_list
