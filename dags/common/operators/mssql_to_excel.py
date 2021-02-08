from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mssql_hook import MsSqlHook
from openpyxl import load_workbook
from os import path

import pandas as pd
import pandas.io.formats.excel


class MsSqlToExcel(BaseOperator):
    """
    Writes data from MsSql to file.
    :param destination_filepath: destination file path.
    :type destination_filepath: str
    :param mssql_source_conn_id: source MsSql connection.
    :type mssql_source_conn_id: str
    :param source_sql: SQL query to execute against the source MsSQL
        database. (templated)
    :type source_sql: str
    :param source_sql_params: Parameters to use in sql query. (templated)
    :type source_sql_params: dict
    :param sheet_name: name of excel sheet (optinal)
    :type sheet_name: str
    :excel_engine: excel engine. default is 'xlsxwriter'
    :excel_engine: str
    :param write_mode: possible options:
        overwrite - creates a new file on top of existing
        append_tab - adds a spreadsheet in an existing document.
    :type write_mode: str
    """

    template_fields = ('source_sql', 'source_sql_params', 'destination_filepath', 'sheet_name')
    template_ext = ('.sql',)
    ui_color = '#d4f4d5'

    @apply_defaults
    def __init__(
            self,
            destination_filepath,
            mssql_source_conn_id,
            source_sql,
            source_sql_params=None,
            sheet_name=None,
            excel_engine='xlsxwriter',
            write_mode='overwrite',
            *args, **kwargs):
        super(MsSqlToExcel, self).__init__(*args, **kwargs)
        if source_sql_params is None:
            source_sql_params = {}
        self.destination_filepath = destination_filepath
        self.mssql_source_conn_id = mssql_source_conn_id
        self.source_sql = source_sql
        self.source_sql_params = source_sql_params
        self.sheet_name = sheet_name
        self.excel_engine = excel_engine
        self.write_mode = write_mode

    def execute(self, context):
        self.log.info("Querying data from source: {0}".format(
            self.mssql_source_conn_id))

        src_mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_source_conn_id)
        df = src_mssql_hook.get_pandas_df(sql=self.source_sql,
                                          parameters=self.source_sql_params)
        rows_total = df.shape[0]

        self.log.info("Writing data to {0}.".format(self.destination_filepath))
        writer = pd.ExcelWriter(self.destination_filepath, engine=self.excel_engine)
        if self.write_mode == 'append_tab' and path.isfile(self.destination_filepath):
            writer.book = load_workbook(self.destination_filepath)

        pandas.io.formats.excel.header_style = None
        if self.sheet_name is not None:
            df.to_excel(writer, sheet_name=self.sheet_name, index=False)
        else:
            df.to_excel(writer, index=False)
        writer.save()

        self.log.info("Total inserted to file: {0} rows".format(rows_total))
