import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from common.utils import rds_utils


class PostgresRDSHook(PostgresHook):

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey',
                            'sslrootcert', 'sslcrl', 'application_name',
                            'keepalives_idle']:
                conn_args[arg_name] = arg_val
            elif arg_name == 'use_iam' and bool(arg_val):
                conn_args['password'] = rds_utils.get_rds_credentials(rds_endpoint=conn.host, user_name=conn.login,
                                                                      port=conn.port)
                conn_args['sslmode'] = 'require'

        self.conn = psycopg2.connect(**conn_args)
        return self.conn
