from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks._mssql_hook import MsSqlHook
from common.utils.etl_utils import apply_transformations

import pandas as pd


class ExcelToMsSql(BaseOperator):
    """
    Transfers data from Excel file to MsSql table.

    :param src_filepath: Source file path.
    :type src_filepath: str
    :param dest_mssql_conn_id: Destination MsSql connection.
    :type dest_mssql_conn_id: str
    :param dest_table: Destination table name
    :type dest_table: str
    :param dest_preoperator: SQL query to execute against the destination before data transfer. (templated)
    :type dest_preoperator: str
    :param dest_preoperator_params: Parameters to use in destination preoperator sql query. (templated)
    :type dest_preoperator_params: dict
    :param sheet_name: Strings are used for sheet names. Integers are used in zero-indexed sheet positions.
        Lists of strings/integers are used to request multiple sheets. Specify None to get all sheets.
    :type sheet_name: str, int, list, or None, default 0
    :param skiprows: Number of header rows to skip from input csv file. Default is 0.
    :type skiprows: int
    :param skipfooter: Number of footer rows to skip from input csv file. Default is 0.
    :type skipfooter: int
    :param dtype: Type name or dict of columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’}
            Default is str for all columns.
    :type dtype: Type name or dict of column
    :param names: A list of column names to use.
        If file contains no header row, then you should explicitly pass header=None.
    :type names: sequence
    :param transformations_templated: A dictionary of templated transformations.
        Example: transformations_templated={"FileDate": "{{ ds }}"}
    :type transformations_templated: dict
    :param transformations: A dictionary of transformations.
        Example: transformations={
            "RENAME:Billingcycle": "BillingCycle",
            "InstitutionID": 1042,
            "AccountNumber": lambda row: row['AccountNumber'].strip(),
            "CustomerName": lambda row: "{} {}".format(row["FirstName"].strip(), row["LastName"].strip()[:100]),
            "Address": lambda row: ctds.SqlVarChar(row["Address"].encode("utf-16le")),
            "FILTER:CheckEmpty": lambda row: row['AccountNumber'].strip() != ""}
    :type transformations: dict
    :param rows_chunk: Number of rows per chunk to commit.
    :type rows_chunk: int
    :param tablock: Table lock hint for fast inserts
    :type tablock: bool

    Returns: total inserted rows
    :type int
    """

    template_fields = ('src_filepath', 'dest_preoperator', 'dest_preoperator_params', 'transformations_templated')
    template_ext = ('.sql',)
    ui_color = '#d4f4d5'

    @apply_defaults
    def __init__(
            self,
            src_filepath,
            dest_mssql_conn_id,
            dest_table,
            dest_preoperator=None,
            dest_preoperator_params=None,
            sheet_name=0,
            skiprows=0,
            skipfooter=0,
            dtype=str,
            names=None,
            transformations_templated=None,
            transformations=None,
            rows_chunk=5000,
            tablock=True,
            *args, **kwargs):
        super(ExcelToMsSql, self).__init__(*args, **kwargs)
        self.src_filepath = src_filepath
        self.dest_mssql_conn_id = dest_mssql_conn_id
        self.dest_table = dest_table
        self.dest_preoperator = dest_preoperator
        self.dest_preoperator_params = dest_preoperator_params
        self.sheet_name = sheet_name
        self.skiprows = skiprows
        self.skipfooter = skipfooter
        self.dtype = dtype
        self.names = names
        self.transformations_templated = transformations_templated
        self.transformations = transformations
        self.rows_chunk = rows_chunk
        self.tablock = tablock

    def _apply_transformations(self, df):
        """"Apply various transformations for each row on CSV data"""
        dest_rows_total = 0
        src_rows_total = df.shape[0]
        transformations = {**(self.transformations or {}), **(self.transformations_templated or {})}

        self.log.info("Applying transformations: {0}".format(transformations))
        self.log.info("Excel field names: {0} ".format(df.columns.tolist()))
        for record in apply_transformations(df, transformations):
            dest_rows_total = dest_rows_total + 1
            yield record

        self.log.info("Total inserted to {0} table: {1} rows".format(self.dest_table, dest_rows_total))
        self.log.info("Total filter out rows: {0}".format(src_rows_total - dest_rows_total))

    def _execute(self, dest_hook):
        args = {"io": self.src_filepath,
                "sheet_name": self.sheet_name,
                "header": None if self.names else 0,
                "names": self.names,
                "dtype": self.dtype,
                "skiprows": self.skiprows,
                "skipfooter": self.skipfooter,
                "keep_default_na": False,
                "na_values": ''}

        self.log.info("Reading data from Excel file with options: {0} ".format(args))
        df = pd.read_excel(**args).fillna('')

        self.log.info("Transferring data from excel file {0}, sheet {1} to table {2}".
                      format(self.src_filepath, self.sheet_name, self.dest_table))
        with dest_hook.get_ctds_conn() as dest_conn:
            rows_total = dest_conn.bulk_insert(
                table=self.dest_table,
                rows=iter(self._apply_transformations(df)),
                batch_size=self.rows_chunk,
                tablock=self.tablock)

        self.log.info("Finished data transfer.")
        return rows_total

    def execute(self, context):
        dest_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_conn_id)

        if self.dest_preoperator:
            self.log.info("Running MSSQL destination preoperator")
            dest_hook.run(sql=self.dest_preoperator,
                          parameters=self.dest_preoperator_params,
                          autocommit=True)

        return self._execute(dest_hook)
