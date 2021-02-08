from airflow.models import Variable
from airflow.exceptions import AirflowException
from common.utils.helpers import dict_merge
from common.utils.os_utils import make_paths

import configparser
import yaml
import os
import os.path as op
import inspect
import ast

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')


def get_config_ini(config_filepath, env):
    parser = configparser.ConfigParser(allow_no_value=True)
    parser.read(config_filepath)
    # Add section if not present
    if not parser.has_section(env):
        parser.add_section(env)
    return parser[env]


def get_config_yaml(config_filepath, env):
    with open(config_filepath, "r") as stream:
        try:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            raise AirflowException('Failed to load {0} file: {1}'.format(config_filepath, str(exc)))
        parser = config.get('DEFAULT', {})
        # Merge default section with environment section if it is present
        dict_merge(parser, config.get(env, {}))
    return parser


def get_config(config_filename='config.ini',
               project_directory=None,
               project_directory_extra=None,
               data_dirs_extra=None,
               make_data_dir=True):
    """
    Returns configuration for an environment from the project configuration file
        and airflow global variables.
    :param config_filename: Name of the configuration file with .ini, .yaml or .yml extension.
        Default is 'config.ini'
    :type config_filename: str
    :param project_directory: Name of the project directory.
        Default is set to the directory name of the calling module
    :type project_directory: str
    :param project_directory_extra: Extra path for the project directory.
    :type project_directory_extra: str
    :param data_dirs_extra: Extra dirs in project directory.
        Overrides the values from config file for key 'data_dirs_extra'
    :type data_dirs_extra: dict
    :param make_data_dir: Creates new data dirs if they doesn't exists.
        Default is to create new data directories.
    :type make_data_dir: bool

    Returns: Configuration for an environment defined with airflow variable "environment"
    :type: dict
    """

    if project_directory is None:
        # Get the directory name of the calling module as default
        project_directory = op.basename(op.dirname(op.abspath(inspect.stack()[1][1])))

    if project_directory_extra:
        # Append extra project directory to project directory
        project_directory = op.join(project_directory, project_directory_extra)

    # Initialize default configuration variables from global airflow variables
    config_vars = {
        "config_path": op.join(Variable.get('config_path', op.join(AIRFLOW_HOME, 'config')), project_directory),
        "sql_path": op.join(Variable.get('sql_path', op.join(AIRFLOW_HOME, 'sql')), project_directory),
        "email_path": op.join(Variable.get('email_path', op.join(AIRFLOW_HOME, 'emails')), project_directory),
        "data_path": op.join(Variable.get('data_path', op.join(AIRFLOW_HOME, 'data')), project_directory),
        "environment": Variable.get('environment', 'LOCAL'),
        "dag_email": Variable.get('dag_email', 'test@domain.com'),
        "dag_email_on_failure": Variable.get('dag_email_on_failure', 'False'),
        "dag_email_on_retry": Variable.get('dag_email_on_retry', 'False')}

    env = config_vars['environment']
    config_file_extension = op.splitext(config_filename)[1]
    config_filepath = op.join(config_vars['config_path'], config_filename)

    # Read the configuration file for an environment
    if config_file_extension == '.ini':
        parser = get_config_ini(config_filepath, env)
    elif config_file_extension in('.yaml', '.yml'):
        parser = get_config_yaml(config_filepath, env)
        config_vars.update({k: v.lower() == 'true' for k, v in config_vars.items() if v.lower() in ('true', 'false')})
    else:
        raise AirflowException('Invalid file extension for {0} file'.format(config_filepath))

    # Override data_dirs_extra from argument to data_dirs_extra from config file
    data_paths = {"data_path": config_vars.get('data_path')}
    dirs = parser.get('data_dirs_extra', {})
    data_dirs_extra = {**(ast.literal_eval(dirs) if isinstance(dirs, str) else dirs), **(data_dirs_extra or {})}

    # Add mapping of extra data dirs and actual paths to config
    if data_dirs_extra:
        data_paths.update({k: op.join(data_paths.get('data_path'), v) for k, v in data_dirs_extra.items()})
        config_vars = {**config_vars, **data_paths}

    # Add fallback values for global config variables
    if isinstance(parser, configparser.SectionProxy):
        _ = {parser.__setitem__(name, val) for name, val in config_vars.items() if parser.get(name) is None}
    else:
        parser = {**config_vars, **parser}

    if make_data_dir:
        make_paths(data_paths.values())

    return parser
