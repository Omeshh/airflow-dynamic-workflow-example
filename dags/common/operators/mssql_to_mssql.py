from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from common.hooks._mssql_hook import MsSqlHook
import ctds


class MsSqlToMsSql(BaseOperator):
    """
    Moves data from MsSql to MsSql.
    :param dest_mssql_conn_id: destination MsSql connection.
    :type dest_mssql_conn_id: str
    :param dest_table: destination table to insert rows.
    :type dest_table: str
    :param src_mssql_conn_id: source MsSql connection.
    :type src_mssql_conn_id: str
    :param src_sql: SQL query to execute against the source MsSQL
        database. (templated)
    :type src_sql: str
    :param src_sql_params: Parameters to use in sql query. (templated)
    :type src_sql_params: dict
    :param dest_preoperator: SQL query to execute against the destination before data transfer. (templated)
    :type dest_preoperator: str
    :param dest_preoperator_params: Parameters to use in destination preoperator sql query. (templated)
    :type dest_preoperator_params: str
    :param rows_chunk: number of rows per chunk to commit.
    :type rows_chunk: int
    """

    template_fields = ('src_sql', 'src_sql_params')
    template_ext = ('.sql',)
    ui_color = '#e3b0ad'

    @apply_defaults
    def __init__(
            self,
            dest_mssql_conn_id,
            dest_table,
            src_mssql_conn_id,
            src_sql,
            src_sql_params=None,
            dest_preoperator=None,
            dest_preoperator_params=None,
            rows_chunk=5000,
            *args, **kwargs):
        super(MsSqlToMsSql, self).__init__(*args, **kwargs)
        if src_sql_params is None:
            src_sql_params = {}
        self.dest_mssql_conn_id = dest_mssql_conn_id
        self.dest_table = dest_table
        self.src_mssql_conn_id = src_mssql_conn_id
        self.src_sql = src_sql
        self.src_sql_params = src_sql_params
        self.dest_preoperator = dest_preoperator
        self.dest_preoperator_params = dest_preoperator_params
        self.rows_chunk = rows_chunk

    def _execute(self, src_hook, dest_hook):
        with src_hook.get_conn() as src_conn, src_conn.cursor() as cursor:
            self.log.info("Querying data from source: {0}".format(
                self.src_mssql_conn_id))
            cursor.execute(self.src_sql, self.src_sql_params)
            target_fields = list(map(lambda field: field[0], cursor.description))

            rows_total = 0
            rows = cursor.fetchmany(self.rows_chunk)
            while len(rows) > 0:
                rows_total = rows_total + len(rows)
                dest_hook.bulk_insert_rows(self.dest_table, rows,
                                           target_fields=target_fields,
                                           commit_every=self.rows_chunk)
                rows = cursor.fetchmany(self.rows_chunk)
                self.log.info("Total inserted: {0} rows".format(rows_total))

            self.log.info("Finished data transfer.")

    def execute(self, context):
        src_hook = MsSqlHook(mssql_conn_id=self.src_mssql_conn_id)
        dest_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_conn_id)

        if self.dest_preoperator:
            self.log.info("Running MSSQL destination preoperator")
            dest_hook.run(sql=self.dest_preoperator,
                          parameters=self.dest_preoperator_params,
                          autocommit=True)

        self._execute(src_hook, dest_hook)


class MsSqlToMsSqlWithLookup(BaseOperator):
    """
    Moves data from MsSql to MsSql based on lookup match output.
    :param dest_mssql_conn_id: destination MsSql connection.
    :type dest_mssql_conn_id: str
    :param dest_table: destination table to insert rows.
    :type dest_table: str
    :param src_mssql_conn_id: source MsSql connection.
    :type src_mssql_conn_id: str
    :param src_sql: SQL query to execute against the source MsSQL
        database. (templated)
    :type src_sql: str
    :param src_sql_params: Parameters to use in sql query. (templated)
    :type src_sql_params: dict
    :param lookup_mssql_conn_id: lookup MsSql connection.
    :type lookup_mssql_conn_id: str
    :param lookup_sql: SQL query to execute against the lookup MsSQL
        database. (templated)
    :type lookup_sql: str
    :param lookup_sql_params: Parameters to use in sql lookup query. (templated)
    :type lookup_sql_params: dict
    :param dest_mssql_no_match_conn_id: destination no match MsSql connection.
    :type dest_mssql_no_match_conn_id: str
    :param dest_no_match_table: destination table to insert non matched rows.
    :type dest_no_match_table: str
    :param rows_chunk: number of rows per chunk to commit.
    :type rows_chunk: int
    """

    template_fields = ('src_sql', 'src_sql_params',
                       'lookup_sql', 'lookup_sql_params',
                       'dest_preoperator', 'dest_preoperator_params',
                       'dest_no_match_preoperator', 'dest_no_match_preoperator_params')
    template_ext = ('.sql',)
    ui_color = '#f1adad'

    @apply_defaults
    def __init__(
            self,
            dest_mssql_conn_id,
            dest_table,
            src_mssql_conn_id,
            src_sql,
            src_sql_params=None,
            lookup_mssql_conn_id=None,
            lookup_sql=None,
            lookup_sql_params=None,
            dest_mssql_no_match_conn_id=None,
            dest_no_match_table=None,
            dest_preoperator=None,
            dest_preoperator_params=None,
            dest_no_match_preoperator=None,
            dest_no_match_preoperator_params=None,
            rows_chunk=5000,
            *args, **kwargs):
        super(MsSqlToMsSqlWithLookup, self).__init__(*args, **kwargs)
        if src_sql_params is None:
            src_sql_params = {}
        self.dest_mssql_conn_id = dest_mssql_conn_id
        self.dest_table = dest_table
        self.src_mssql_conn_id = src_mssql_conn_id
        self.src_sql = src_sql
        self.src_sql_params = src_sql_params
        self.lookup_mssql_conn_id = lookup_mssql_conn_id
        self.lookup_sql = lookup_sql
        self.lookup_sql_params = lookup_sql_params
        self.dest_mssql_no_match_conn_id = dest_mssql_no_match_conn_id
        self.dest_no_match_table = dest_no_match_table
        self.dest_preoperator = dest_preoperator
        self.dest_preoperator_params = dest_preoperator_params
        self.dest_no_match_preoperator = dest_no_match_preoperator
        self.dest_no_match_preoperator_params = dest_no_match_preoperator_params
        self.rows_chunk = rows_chunk

    def _execute(self, src_hook, lookup_hook, dest_hook, dest_no_match_hook):
        with src_hook.get_conn() as src_conn:
            cursor = src_conn.cursor()
            self.log.info("Querying data from source: {0}".format(self.src_mssql_conn_id))
            cursor.execute(self.src_sql, self.src_sql_params)
            target_fields = list(map(lambda field: field[0], cursor.description))
            rows = cursor.fetchmany(self.rows_chunk)

            with lookup_hook.get_conn() as lkp_conn:
                rows_total, rows_total_no_match = 0, 0
                merged_target_fields = target_fields
                lkp_cursor = lkp_conn.cursor()

                while len(rows) > 0:
                    rows_match, rows_no_match = [], []

                    for i, row in enumerate(rows):
                        lkp_sql_params = {k: rows[i][target_fields.index(v)]
                                          for k, v in self.lookup_sql_params.items()}
                        lkp_cursor.execute(self.lookup_sql, lkp_sql_params)
                        lkp_target_fields = list(map(lambda field: field[0], lkp_cursor.description))
                        merged_target_fields = target_fields + lkp_target_fields
                        lkp_row = lkp_cursor.fetchone()

                        if lkp_row is not None:
                            rows_total = rows_total + 1
                            rows_match.append(rows[i]+lkp_row)
                        else:
                            rows_total_no_match = rows_total_no_match + 1
                            rows_no_match.append(rows[i])

                    dest_hook.bulk_insert_rows(self.dest_table,
                                               rows_match,
                                               target_fields=merged_target_fields,
                                               commit_every=self.rows_chunk)

                    if dest_no_match_hook is not None:
                        dest_no_match_hook.bulk_insert_rows(self.dest_no_match_table,
                                                            rows_no_match,
                                                            target_fields=target_fields,
                                                            commit_every=self.rows_chunk)

                    rows = cursor.fetchmany(self.rows_chunk)

                self.log.info("Total inserted: {0} rows".format(rows_total))
                self.log.info("Total inserted for no match: {0} rows".format(rows_total_no_match))

        self.log.info("Finished data transfer.")

        return {"rows_total": rows_total, "rows_total_no_match": rows_total_no_match}

    def execute(self, context):
        src_hook = MsSqlHook(mssql_conn_id=self.src_mssql_conn_id)
        lookup_hook = MsSqlHook(mssql_conn_id=self.lookup_mssql_conn_id)
        dest_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_conn_id)
        dest_no_match_hook = None

        if self.dest_mssql_no_match_conn_id is not None:
            dest_no_match_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_no_match_conn_id)

            if self.dest_no_match_preoperator:
                self.log.info("Running MSSQL destination no match preoperator")
                dest_no_match_hook.run(sql=self.dest_no_match_preoperator,
                                       parameters=self.dest_no_match_preoperator_params,
                                       autocommit=True)

        if self.dest_preoperator:
            self.log.info("Running MSSQL destination preoperator")
            dest_hook.run(sql=self.dest_preoperator,
                          parameters=self.dest_preoperator_params,
                          autocommit=True)

        return self._execute(src_hook, lookup_hook, dest_hook, dest_no_match_hook)


class MsSqlToMsSqlUsingCTDS(BaseOperator):
    """
    Moves data from MsSql to MsSql with bulk insert using cTDS library based on optional lookup match output.
    cTDS docs: https://pypi.org/project/ctds/
    Limitations: * Money data type is not supported in destination table, use DECIMAL(19,4) instead
        * This operator currently only supports one type of encoding for all columns in destination table and
        mixed varchar / nvarchar data type columns are not supported.
        * Column names (case sensitive) and data types in destination table should match exactly
        to the source data.
        * Data truncation errors for (n)varchar columns are currently ignored by bulk_insert.
    :param dest_mssql_conn_id: destination MsSql connection.
    :type dest_mssql_conn_id: str
    :param dest_table: destination table to insert rows.
    :type dest_table: str
    :param src_mssql_conn_id: source MsSql connection.
    :type src_mssql_conn_id: str
    :param src_sql: SQL query to execute against the source MsSQL
        database. (templated)
    :type src_sql: str
    :param src_sql_params: Parameters to use in sql query. (templated)
    :type src_sql_params: dict
    :param lookup_mssql_conn_id: lookup MsSql connection.
    :type lookup_mssql_conn_id: str
    :param lookup_sql: SQL query to execute against the lookup MsSQL
        database. (templated)
    :type lookup_sql: str
    :param lookup_sql_params: Parameters to use in sql lookup query. (templated)
    :type lookup_sql_params: list
    :param dest_mssql_no_match_conn_id: destination no match MsSql connection.
    :type dest_mssql_no_match_conn_id: str
    :param dest_no_match_table: destination table to insert non matched rows.
    :type dest_no_match_table: str
    :param rows_chunk: number of rows per chunk to commit.
    :type rows_chunk: int
    :param tablock: Table lock hint for fast inserts
    :type tablock: bool
    :param bulk_insert_dict_rows: ctds 1.9 supports passing rows as dict objects, mapping column name
        to value. This is useful if the table contains NULLable columns not present in the source data.
    :type bulk_insert_dict_rows: bool
    :param dest_character_encoding: pass 'utf-16le' for nvarchar and 'latin-1' for varchar columns,
        mixed columns are currently not supported. Pass empty value '' if no encoding is required.
    :type dest_character_encoding: str
    """

    template_fields = ('src_sql', 'src_sql_params',
                       'lookup_sql', 'lookup_sql_params',
                       'dest_preoperator', 'dest_preoperator_params',
                       'dest_no_match_preoperator', 'dest_no_match_preoperator_params')
    template_ext = ('.sql',)
    ui_color = '#f1adad'

    @apply_defaults
    def __init__(
            self,
            dest_mssql_conn_id,
            dest_table,
            src_mssql_conn_id,
            src_sql,
            src_sql_params=None,
            lookup_mssql_conn_id=None,
            lookup_sql=None,
            lookup_sql_params=None,
            dest_mssql_no_match_conn_id=None,
            dest_no_match_table=None,
            dest_preoperator=None,
            dest_preoperator_params=None,
            dest_no_match_preoperator=None,
            dest_no_match_preoperator_params=None,
            rows_chunk=10000,
            tablock=True,
            bulk_insert_dict_rows=True,
            dest_character_encoding='utf-16le',
            *args, **kwargs):
        super(MsSqlToMsSqlUsingCTDS, self).__init__(*args, **kwargs)
        if src_sql_params is None:
            src_sql_params = {}
        self.dest_mssql_conn_id = dest_mssql_conn_id
        self.dest_table = dest_table
        self.src_mssql_conn_id = src_mssql_conn_id
        self.src_sql = src_sql
        self.src_sql_params = src_sql_params
        self.lookup_mssql_conn_id = lookup_mssql_conn_id
        self.lookup_sql = lookup_sql
        self.lookup_sql_params = lookup_sql_params
        self.dest_mssql_no_match_conn_id = dest_mssql_no_match_conn_id
        self.dest_no_match_table = dest_no_match_table
        self.dest_preoperator = dest_preoperator
        self.dest_preoperator_params = dest_preoperator_params
        self.dest_no_match_preoperator = dest_no_match_preoperator
        self.dest_no_match_preoperator_params = dest_no_match_preoperator_params
        self.rows_chunk = rows_chunk
        self.tablock = tablock
        self.bulk_insert_dict_rows = bulk_insert_dict_rows
        self.dest_character_encoding = dest_character_encoding

    def _encode_result(self, rows, target_fields=None):
        result = [(ctds.SqlVarChar(col.encode(self.dest_character_encoding))
                   if isinstance(col, str) else col
                   for col in tuple(row))
                  for row in rows]

        if self.bulk_insert_dict_rows:
            result = [dict(zip(target_fields, tuple(row)))
                      for row in result]

        return result

    def _execute(self, src_hook, lookup_hook, dest_hook, dest_no_match_hook):
        with src_hook.get_ctds_conn() as src_conn, src_conn.cursor() as src_cursor:
            self.log.info("Querying data from source: {0}".format(self.src_mssql_conn_id))
            src_cursor.execute(self.src_sql, self.src_sql_params)
            src_columns = [column.name for column in src_cursor.description]
            rows = src_cursor.fetchmany(self.rows_chunk)

            with dest_hook.get_ctds_conn() as dest_conn:
                rows_total, rows_total_match, rows_total_no_match = 0, 0, 0
                target_fields = src_columns

                while len(rows) > 0:
                    rows_match, rows_no_match = [], []
                    rows_total = rows_total + len(rows)
                    self.log.info("Total source rows: {0} rows".format(rows_total))

                    if lookup_hook is not None:
                        with lookup_hook.get_ctds_conn() as lkp_conn, lkp_conn.cursor() as lkp_cursor:
                            for row in rows:
                                lkp_sql_params = {v: row[src_columns.index(v)]
                                                  for v in self.lookup_sql_params}
                                lkp_cursor.execute(self.lookup_sql, lkp_sql_params)
                                lkp_columns = [column.name for column in lkp_cursor.description]
                                target_fields = src_columns + lkp_columns
                                lkp_row = lkp_cursor.fetchone()

                                if lkp_row is not None:
                                    rows_match.append(tuple(row) + tuple(lkp_row))
                                elif dest_no_match_hook is not None:
                                    rows_no_match.append(row)
                    else:
                        rows_match = rows

                    if self.dest_character_encoding:
                        rows_match = self._encode_result(rows_match, target_fields)

                    row_count = dest_conn.bulk_insert(
                        table=self.dest_table,
                        rows=rows_match,
                        batch_size=self.rows_chunk,
                        tablock=self.tablock)
                    rows_total_match = rows_total_match + row_count
                    self.log.info("Total inserted: {0} rows".format(rows_total_match))

                    if dest_no_match_hook is not None:
                        if self.dest_character_encoding:
                            rows_no_match = self._encode_result(rows_no_match, src_columns)

                        with dest_no_match_hook.get_ctds_conn() as dest_conn_no_match:
                            row_count_no_match = dest_conn_no_match.bulk_insert(
                                table=self.dest_no_match_table,
                                rows=rows_no_match,
                                batch_size=self.rows_chunk,
                                tablock=self.tablock)

                            rows_total_no_match = rows_total_no_match + row_count_no_match
                            self.log.info("Total inserted for no match: {0} rows".format(rows_total_no_match))

                    rows = src_cursor.fetchmany(self.rows_chunk)

        self.log.info("Finished data transfer.")
        return {"rows_total": rows_total,
                "rows_total_match": rows_total_match,
                "rows_total_no_match": rows_total_no_match}

    def execute(self, context):
        src_hook = MsSqlHook(mssql_conn_id=self.src_mssql_conn_id)
        dest_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_conn_id)
        dest_no_match_hook = None
        lookup_hook = None

        if self.lookup_mssql_conn_id is not None:
            lookup_hook = MsSqlHook(mssql_conn_id=self.lookup_mssql_conn_id)

        if self.dest_mssql_no_match_conn_id is not None:
            dest_no_match_hook = MsSqlHook(mssql_conn_id=self.dest_mssql_no_match_conn_id)

            if self.dest_no_match_preoperator:
                self.log.info("Running MSSQL destination no match preoperator")
                dest_no_match_hook.run(sql=self.dest_no_match_preoperator,
                                       parameters=self.dest_no_match_preoperator_params,
                                       autocommit=True)

        if self.dest_preoperator:
            self.log.info("Running MSSQL destination preoperator")
            dest_hook.run(sql=self.dest_preoperator,
                          parameters=self.dest_preoperator_params,
                          autocommit=True)

        return self._execute(src_hook, lookup_hook, dest_hook, dest_no_match_hook)
