from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mssql_hook import MsSqlHook


class MsSqlToCSV(BaseOperator):
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
    :param sep: default ','
            Field delimiter for the output file.
    :type sep: character

    Returns: rows_total
    :type int
    """

    template_fields = ('source_sql', 'source_sql_params', 'destination_filepath')
    template_ext = ('.sql',)
    ui_color = '#d4f4d5'

    @apply_defaults
    def __init__(
            self,
            destination_filepath,
            mssql_source_conn_id,
            source_sql,
            source_sql_params=None,
            sep=",",
            *args, **kwargs):
        super(MsSqlToCSV, self).__init__(*args, **kwargs)
        if source_sql_params is None:
            source_sql_params = {}
        self.destination_filepath = destination_filepath
        self.mssql_source_conn_id = mssql_source_conn_id
        self.source_sql = source_sql
        self.source_sql_params = source_sql_params
        self.sep = sep

    def execute(self, context):
        self.log.info("Querying data from source: {0}".format(
            self.mssql_source_conn_id))

        src_mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_source_conn_id)
        df = src_mssql_hook.get_pandas_df(sql=self.source_sql,
                                          parameters=self.source_sql_params)
        rows_total = df.shape[0]

        self.log.info("Writing data to {0}.".format(self.destination_filepath))
        df.to_csv(self.destination_filepath, sep=self.sep, index=False)

        self.log.info("Total inserted to file: {0} rows".format(rows_total))

        return rows_total