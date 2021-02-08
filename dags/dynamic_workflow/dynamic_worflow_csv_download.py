import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from common.operators.zip_operator import UnzipOperator
from common.operators.cryptography_operator import CryptographyOperator
from common.operators.ftp_search_operator import FTPSearchOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from common.operators.local_to_s3_operator import LocalToS3Operator

from common.utils.configuration import get_config
from common.utils.helpers import dict_merge
import copy
import re


def remove_local_files(download_directory, file_list_location, **kwargs):
    file_list = kwargs['task_instance'].xcom_pull(file_list_location)
    if file_list is not None:
        for f in file_list:
            full_name = download_directory + "/" + f
            if os.path.exists(full_name):
                os.remove(full_name)


def skip_if_no_files(search_expr=None, **kwargs):
    file_list = [file_name for file_name in kwargs['task_instance'].xcom_pull('ftp_download') or []
                 if (search_expr is None or re.search(search_expr, file_name))]
    if file_list is None or not bool(file_list):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException
    return file_list


def parse_directory_pattern(directory_pattern, date, file_type):
    return directory_pattern \
        .replace("<date>", date) \
        .replace("<type>", file_type)


def create_dag(dag_id, description, conf, date):
    default_args = {
        'owner': 'airflow',
        'email': conf.get('dag_email').split(','),
        'email_on_failure': conf.get('dag_email_on_failure'),
        'email_on_retry': conf.get('dag_email_on_retry'),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': conf.get('dag_depends_on_past'),
    }

    dag = DAG(
        dag_id=dag_id,
        description=description,
        schedule_interval=conf.get('dag_schedule_interval'),
        template_searchpath=[conf.get('sql_path'), conf.get('email_path')],
        default_args=default_args,
        start_date=datetime(*map(int, conf.get('dag_start_date').split(','))),
        catchup=conf.get('dag_catchup', True))

    with dag:
        misc_search_expr = download_search_expr = None
        search_exprs = [file_spec.get('search_expr') for file_spec in conf.get('file_specs').values()]
        if None in search_exprs:
            misc_search_expr = '^(?!{})'.format('|'.join([s for s in search_exprs if s is not None]))
        else:
            download_search_expr = '({})'.format('|'.join([s for s in search_exprs if s is not None]))

        download = FTPSearchOperator(
            task_id='ftp_download',
            ftp_conn_id=conf.get('ftp_conn_id'),
            local_filepath=conf.get('data_path'),
            remote_filepath=conf.get('remote_inbound_path'),
            search_expr=download_search_expr,
            min_date="{{ execution_date }}",
            max_date="{{ next_execution_date }}",
            ftp_conn_type=conf.get('ftp_conn_type'))

        remove_tmp_files = PythonOperator(
            task_id='remove_local_files',
            provide_context=True,
            python_callable=remove_local_files,
            op_kwargs={"download_directory": conf.get('data_path'),
                       "file_list_location": download.task_id},
            trigger_rule='none_failed')

        for filename, file_spec in conf.get('file_specs').items():
            file_spec = {**{k: v for k, v in conf.items()
                            if k in ('search_expr', 'gpg_decrypt', 'gpg_decrypt', 'unzip', 'import',
                                     'output_date_format')},
                         **file_spec}
            date_str = date.strftime(file_spec.get('output_date_format', '%Y-%m-%d'))
            input_s3_dir = "s3://{}/{}".format(conf.get('s3_bucket'),
                                               parse_directory_pattern(file_spec['directory_pattern'],
                                                                       date_str, 'csv').lstrip("/"))

            check_for_files = PythonOperator(
                task_id='check_for_{}_files'.format(filename),
                provide_context=True,
                python_callable=skip_if_no_files,
                op_kwargs={"search_expr": file_spec.get('search_expr') or misc_search_expr})
            file_list_xcom_location = check_for_files.task_id

            if file_spec.get('unzip'):
                unzip_files = UnzipOperator(task_id='unzip_{}_files'.format(filename),
                                            file_list_xcom_location=file_list_xcom_location)
                file_list_xcom_location = unzip_files.task_id
            else:
                unzip_files = DummyOperator(task_id='unzip_{}_files'.format(filename))

            if file_spec.get('gpg_decrypt'):
                decrypt = CryptographyOperator(
                    task_id='decrypt_{}_files'.format(filename),
                    crypto_conn_id=conf.get('crypt_conn'),
                    file_list_xcom_location=file_list_xcom_location,
                    output_directory=conf.get('data_path'),
                    remove_encrypted=True,
                    operation='decrypt')
                file_list_xcom_location = decrypt.task_id
            else:
                decrypt = DummyOperator(task_id='decrypt_{}_files'.format(filename))

            save_to_s3 = LocalToS3Operator(
                task_id='save_{}_files_to_s3'.format(filename),
                s3_conn_id=conf.get('aws_connection_id'),
                s3_bucket=conf.get('s3_bucket'),
                s3_prefix=input_s3_dir,
                file_list_xcom_location=file_list_xcom_location)

            if file_spec.get('import'):
                import_file = DatabricksSubmitRunOperator(
                    task_id='import_{}_file'.format(filename),
                    job_id='{}dynamic_workflow_file_import'.format(conf.get('databricks_job_prefix')),
                    polling_period_seconds=60 * 3,
                    notebook_params={"config_path": file_spec['config_path'],
                                     "file_date": date_str,
                                     "file_path": "{}/{}".format(
                                         input_s3_dir, str(file_spec.get('unzipped_search_expr',
                                                                         file_spec['search_expr'])).replace(".*", "*"))}
                )

                save_to_s3 >> import_file

            download >> check_for_files >> unzip_files >> decrypt >> save_to_s3 >> remove_tmp_files
    return dag


today_date = datetime.now()

# Get configuration settings for a project
base_conf = get_config(config_filename='config.yaml')

for config_filename in base_conf.get('config_filenames'):
    config = copy.deepcopy(base_conf)
    conf = get_config(config_filename=config_filename)
    dict_merge(config, conf)
    dag_id = config.get('dag_id').format(config_filename=config_filename.split('.')[0])

    globals()[dag_id] = create_dag(
        dag_id=dag_id,
        description='DAG to download files from {} and load to database'.format(config_filename.split('.')[0]),
        conf=config,
        date=today_date)
