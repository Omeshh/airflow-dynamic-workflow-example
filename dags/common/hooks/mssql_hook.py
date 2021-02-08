import _tds
import pymssql
import sys
from contextlib import closing
from datetime import datetime
from pprint import pprint

import ctds
import numpy
from airflow.exceptions import AirflowException
from airflow.hooks.dbapi_hook import DbApiHook
from builtins import str
from past.builtins import basestring


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.
    """

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MsSqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        """
        Returns a mssql connection object
        """
        db = self.get_connection(self.mssql_conn_id)
        conn = pymssql.connect(
            server=db.host,
            user=db.login,
            password=db.password,
            database=self.schema or db.schema,
            port=db.port)
        return conn

    def get_ctds_conn(self):
        """
        Returns a mssql connection object
        https://pypi.org/project/ctds/
        """
        db = self.get_connection(self.mssql_conn_id)
        conn = ctds.connect(
            server=db.host,
            user=db.login,
            password=db.password,
            database=self.schema or db.schema,
            port=db.port,
            login_timeout=30,
            timeout=60 * 60,
            autocommit=True,
            paramstyle='named')
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)

    def get_records_dict(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                columns = [column[0] for column in cur.description]
                results = []
                for row in cur.fetchall():
                    results.append(dict(zip(columns, row)))
                return results

    def get_first_dict(self, sql, parameters=None):
        """
        Executes the sql and returns the first resulting row.
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return dict(zip([column[0] for column in cur.description],
                                cur.fetchone()))

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        Changes from standard DbApiHook implementation:
        - SQL queries in MsSQL can not be terminated with a semicolon (';')
        - Replace NaN values with NULL using numpy.nan_to_num (not using is_nan()
          because of input types error for strings)
        - Coerce datetime cells to MsSQL DATETIME format during insert
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = '({})'.format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        if self.supports_autocommit:
            self.set_autocommit(conn, autocommit=False)
        conn.commit()
        i = 0
        for row in rows:
            i += 1
            lst = []
            for cell in row:
                if isinstance(cell, basestring):
                    lst.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    lst.append('NULL')
                elif type(cell) == float and \
                        numpy.isnan(cell):  # coerce numpy NaN to NULL
                    lst.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    lst.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    lst.append("to_date('" +
                               cell.strftime('%Y-%m-%d %H:%M:%S') +
                               "','YYYY-MM-DD HH24:MI:SS')")
                else:
                    lst.append(str(cell))
            values = tuple(lst)
            sql = 'INSERT /*+ APPEND */ ' \
                  'INTO {0} {1} VALUES ({2})'.format(table,
                                                     target_fields,
                                                     ','.join(values))
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()
                self.log.info('Loaded {i} into {table} rows so far'.format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        self.log.info('Done loading. Loaded a total of {i} rows'.format(**locals()))

    def bulk_insert_rows(self, table, rows, target_fields=None, commit_every=5000):
        """
        A performant bulk insert for cx_Oracle
        that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        values = ', '.join('%s' for _ in range(1, len(target_fields) + 1))
        prepared_stm = 'insert into {tablename} ({columns}) values ({values})'.format(
            tablename=table,
            columns=', '.join(target_fields),
            values=values,
        )
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                # cursor.prepare(prepared_stm)
                cursor.executemany(prepared_stm, row_chunk)
                conn.commit()
                self.log.info('[%s] inserted %s rows', table, row_count)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        # cursor.prepare(prepared_stm)
        cursor.executemany(prepared_stm, row_chunk)
        conn.commit()
        self.log.info('[%s] inserted %s rows', table, row_count)
        cursor.close()
        conn.close()

    def bulk_insert_rows_ctds(self, table, rows, target_fields, commit_every=5000):
        """
        ;param table: Name of the target table
        ;type  table: str
        ;param rows: The rows to insert into the table, data types being correct is important
        ;type  rows: iterable of tuples
        ;param target_fields: The names of the columns to fill in the table
        ;type  target_fields: iterable of strings
        ;param commit_every: An optional batch size.
        ;type  commit_every: int
        """

        if len(rows):
            with closing(self.get_ctds_conn()) as conn:
                encoded_rows = [(ctds.SqlVarChar(col.encode('utf-16le')) if isinstance(col, basestring) else col
                                 for col in tuple(row)) for row in rows]
                data = [dict(zip(target_fields, tuple(row))) for row in encoded_rows]

                try:
                    rows_saved = conn.bulk_insert(table=table, rows=data, batch_size=commit_every, tablock=True)
                    if rows_saved != len(rows):
                        self.log.error('Table: {}'.format(table))
                        pprint(data)
                        raise AirflowException('ERROR bulk_insert only = {} should have been {}'
                                               .format(rows_saved, len(rows)))
                except _tds.DatabaseError:
                    self.log.error('Table: {}'.format(table))
                    pprint(data)
                    raise AirflowException('ERROR DatabaseError: '.format(rows_saved, len(rows)))

    def run(self, sql, autocommit=False, parameters=None):
        return super(MsSqlHook, self).run(sql, autocommit=autocommit, parameters=parameters)

    def get_records(self, sql, parameters=None):
        return super(MsSqlHook, self).get_records(sql=sql, parameters=parameters)
