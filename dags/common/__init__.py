from airflow.plugins_manager import AirflowPlugin

from common.operators.mssql_to_mssql import MsSqlToMsSql
from common.operators.mssql_to_mssql import MsSqlToMsSqlWithLookup
from common.operators.mssql_to_mssql import MsSqlToMsSqlUsingCTDS
from common.operators.mssql_to_csv import MsSqlToCSV
from common.operators.csv_to_mssql import CSVToMsSql
from common.operators.excel_to_mssql import ExcelToMsSql
from common.operators.zip_operator import ZipOperator, UnzipOperator
from common.operators.cryptography_operator import CryptographyOperator


class Plugin(AirflowPlugin):
    """
    Defines custom airflow plugins.
    """
    name = "Plugin"
    operators = [MsSqlToMsSql,
                 MsSqlToMsSqlWithLookup,
                 MsSqlToMsSqlUsingCTDS,
                 MsSqlToCSV,
                 CSVToMsSql,
                 ExcelToMsSql,
                 ZipOperator,
                 UnzipOperator,
                 CryptographyOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
