from airflow.operators.mssql_operator import MsSqlOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.hooks._mssql_hook import MsSqlHook


class MsSqlOperator(MsSqlOperator):
    """
    Extend MsSqlOperator with modified templated fields.
    """

    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#f9a75e'


class MsSqlGetRecordsOperator(BaseOperator):
    """
    Get result set from sql code execution in a specific Microsoft SQL database

    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file with .sql extension
    :param parameters: The parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: string
    :param is_dict_result_set: the format of the output result set
    :type is_dict_result_set: bool
    :param is_single_row_result_set: set true if the expected result set is a single row else set false
    :type is_single_row_result_set: bool
    """

    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#a9d89c'

    @apply_defaults
    def __init__(
            self,
            sql,
            mssql_conn_id='mssql_default',
            parameters=None,
            database=None,
            is_dict_result_set=True,
            is_single_row_result_set=False,
            *args, **kwargs):
        super(MsSqlGetRecordsOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.database = database
        self.is_dict_result_set = is_dict_result_set
        self.is_single_row_result_set = is_single_row_result_set

    def execute(self, context):
        self.log.info('Running: {0} with params: {1}'.format(self.sql, self.parameters))
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id,
                                schema=self.database)

        if self.is_dict_result_set:
            if self.is_single_row_result_set:
                result = hook.get_first_dict(sql=self.sql,
                                             parameters=self.parameters)
            else:
                result = hook.get_records_dict(sql=self.sql,
                                               parameters=self.parameters)
        else:
            if self.is_single_row_result_set:
                result = hook.get_first(sql=self.sql,
                                        parameters=self.parameters)
            else:
                result = hook.get_records(sql=self.sql,
                                          parameters=self.parameters)
        return result
