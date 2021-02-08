from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.utils.decorators import apply_defaults


class MSSqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying until
    sql returns at least one row and the first row first column value is 1.
    :param conn_id: The connection to run the sensor against
    :type conn_id: str
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :type sql: str
    """
    template_fields = ('sql', 'params')
    template_ext = ('.sql',)
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql,
                 params=None,
                 *args,
                 **kwargs):
        super(MSSqlSensor, self).__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.params = params
        # self.poke_interval = 60 * 5
        # self.timeout = 60 * 60 * 12

    def poke(self, context):
        hook = MsSqlHook.get_connection(self.conn_id).get_hook()

        record = hook.get_first(sql=self.sql, parameters=self.params)
        if not record:
            return False
        return True if record[0] == 1 else False
