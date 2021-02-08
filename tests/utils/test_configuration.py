import unittest
from unittest.mock import patch, call
from common.utils import configuration
import configparser
import io


class TestConfiguration(unittest.TestCase):
    def setUp(self):
        self.config_vars = {
            'config_path': '/usr/local/airflow/config',
            'sql_path': '/usr/local/airflow/sql',
            'email_path': '/usr/local/airflow/emails',
            'data_path': '/usr/local/airflow/data',
            'environment': 'DEV',
            'dag_email': 'test@domain.com',
            'dag_email_on_failure': 'False',
            'dag_email_on_retry': 'False'}

        self.config_ini = """
        [DEFAULT]
        ; test comment
        var1: value1
        var2: value2
    
        [DEV]
        var1: value_dev1
        var_dev2: value_dev2
        data_dirs_extra: {"test_path1": "dir_dev1", "test_path_dev2": "dir_dev2"}
        """

        self.config_yaml = """
        DEFAULT:
        # test comment
          var1: value1
          data_dirs_extra:
            test_path1: dir1
    
        DEV:
          var1: value_dev1
          var_dev2: value_dev2
          var_list_dev:
            - item1_dev
            - item2_dev
          data_dirs_extra:
            test_path1: dir_dev1
            test_path_dev2: dir_dev2
        """

        self.config_ini_missing_section = """
        [DEFAULT]
        var1: value1
        """

        self.config_yaml_missing_section = """
        DEFAULT:
          var1: value1
        """
        self.expected_missing_section = {
            'config_path': '/usr/local/airflow/config/utils',
            'dag_email': 'test@domain.com',
            'dag_email_on_failure': 'False',
            'dag_email_on_retry': 'False',
            'data_path': '/usr/local/airflow/data/utils',
            'email_path': '/usr/local/airflow/emails/utils',
            'environment': 'DEV',
            'sql_path': '/usr/local/airflow/sql/utils',
            'var1': 'value1'}

        self.expected_missing_section_yaml = {
            'config_path': '/usr/local/airflow/config/utils',
            'dag_email': 'test@domain.com',
            'dag_email_on_failure': False,
            'dag_email_on_retry': False,
            'data_path': '/usr/local/airflow/data/utils',
            'email_path': '/usr/local/airflow/emails/utils',
            'environment': 'DEV',
            'sql_path': '/usr/local/airflow/sql/utils',
            'var1': 'value1'}

    @patch('builtins.open')
    def test_get_config_ini(self, mock_file):
        mock_file.return_value = io.StringIO(self.config_ini)
        conf = configuration.get_config_ini('/usr/local/airflow/config/utils/config.ini', 'DEV')

        mock_file.assert_called_once_with('/usr/local/airflow/config/utils/config.ini', encoding=None)
        self.assertIsInstance(conf, configparser.SectionProxy)
        self.assertEqual(
            dict(conf),
            {'data_dirs_extra': '{"test_path1": "dir_dev1", "test_path_dev2": "dir_dev2"}',
             'var1': 'value_dev1',
             'var2': 'value2',
             'var_dev2': 'value_dev2'})

    @patch('builtins.open')
    def test_get_config_yaml(self, mock_file):
        mock_file.return_value = io.StringIO(self.config_yaml)
        conf = configuration.get_config_yaml('/usr/local/airflow/config/utils/config.yaml', 'DEV')

        mock_file.assert_called_with('/usr/local/airflow/config/utils/config.yaml', 'r')
        self.assertIsInstance(conf, dict)
        self.assertEqual(
            conf,
            {'data_dirs_extra': {'test_path1': 'dir_dev1', 'test_path_dev2': 'dir_dev2'},
             'var1': 'value_dev1',
             'var_dev2': 'value_dev2',
             'var_list_dev': ['item1_dev', 'item2_dev']})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_ini_format(self, mock_get, mock_file, mock_os_makedirs):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini)
        conf = configuration.get_config(config_filename='config.ini')

        mock_file.assert_called_with('/usr/local/airflow/config/utils/config.ini', encoding=None)
        self.assertIsInstance(conf, configparser.SectionProxy)
        self.assertEqual(
            mock_os_makedirs.mock_calls,
            [call('/usr/local/airflow/data/utils', exist_ok=True),
             call('/usr/local/airflow/data/utils/dir_dev1', exist_ok=True),
             call('/usr/local/airflow/data/utils/dir_dev2', exist_ok=True)])
        self.assertEqual(
            dict(conf),
            {'config_path': '/usr/local/airflow/config/utils',
             'dag_email': 'test@domain.com',
             'dag_email_on_failure': 'False',
             'dag_email_on_retry': 'False',
             'data_dirs_extra': '{"test_path1": "dir_dev1", "test_path_dev2": "dir_dev2"}',
             'data_path': '/usr/local/airflow/data/utils',
             'email_path': '/usr/local/airflow/emails/utils',
             'environment': 'DEV',
             'sql_path': '/usr/local/airflow/sql/utils',
             'test_path1': '/usr/local/airflow/data/utils/dir_dev1',
             'test_path_dev2': '/usr/local/airflow/data/utils/dir_dev2',
             'var1': 'value_dev1',
             'var2': 'value2',
             'var_dev2': 'value_dev2'})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_yaml_format(self, mock_get, mock_file, mock_os_makedirs):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_yaml)
        conf = configuration.get_config(config_filename='config.yaml')

        mock_file.assert_called_with('/usr/local/airflow/config/utils/config.yaml', 'r')
        self.assertEqual(
            mock_os_makedirs.mock_calls,
            [call('/usr/local/airflow/data/utils', exist_ok=True),
             call('/usr/local/airflow/data/utils/dir_dev1', exist_ok=True),
             call('/usr/local/airflow/data/utils/dir_dev2', exist_ok=True)])

        self.assertEqual(
            conf,
            {'config_path': '/usr/local/airflow/config/utils',
             'dag_email': 'test@domain.com',
             'dag_email_on_failure': False,
             'dag_email_on_retry': False,
             'data_dirs_extra': {'test_path1': 'dir_dev1', 'test_path_dev2': 'dir_dev2'},
             'data_path': '/usr/local/airflow/data/utils',
             'email_path': '/usr/local/airflow/emails/utils',
             'environment': 'DEV',
             'sql_path': '/usr/local/airflow/sql/utils',
             'test_path1': '/usr/local/airflow/data/utils/dir_dev1',
             'test_path_dev2': '/usr/local/airflow/data/utils/dir_dev2',
             'var1': 'value_dev1',
             'var_dev2': 'value_dev2',
             'var_list_dev': ['item1_dev', 'item2_dev']})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_yml_extension(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_yaml)
        configuration.get_config(config_filename='config.yml')

        mock_file.assert_called_with('/usr/local/airflow/config/utils/config.yml', 'r')

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_ini_format_missing_section(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        conf = configuration.get_config(config_filename='config.ini')

        self.assertEqual(dict(conf), self.expected_missing_section)

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_yaml_format_missing_section(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_yaml_missing_section)
        conf = configuration.get_config(config_filename='config.yaml')

        self.assertEqual(dict(conf), self.expected_missing_section_yaml)

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_project_directory(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        conf = configuration.get_config(config_filename='config.ini', project_directory='project_dir')

        self.assertEqual(
            dict(conf),
            {'config_path': '/usr/local/airflow/config/project_dir',
             'dag_email': 'test@domain.com',
             'dag_email_on_failure': 'False',
             'dag_email_on_retry': 'False',
             'data_path': '/usr/local/airflow/data/project_dir',
             'email_path': '/usr/local/airflow/emails/project_dir',
             'environment': 'DEV',
             'sql_path': '/usr/local/airflow/sql/project_dir',
             'var1': 'value1'})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_project_directory_extra(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        conf = configuration.get_config(config_filename='config.ini',
                                        project_directory_extra=r'project_dir/project_dir_nested')

        self.assertEqual(
            dict(conf),
            {'config_path': '/usr/local/airflow/config/utils/project_dir/project_dir_nested',
             'dag_email': 'test@domain.com',
             'dag_email_on_failure': 'False',
             'dag_email_on_retry': 'False',
             'data_path': '/usr/local/airflow/data/utils/project_dir/project_dir_nested',
             'email_path': '/usr/local/airflow/emails/utils/project_dir/project_dir_nested',
             'environment': 'DEV',
             'sql_path': '/usr/local/airflow/sql/utils/project_dir/project_dir_nested',
             'var1': 'value1'})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_data_dirs_extra(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        conf = configuration.get_config(config_filename='config.ini',
                                        data_dirs_extra={'test_path1': 'dir_dev1',
                                                         'test_path_dev2': 'dir_dev2'})

        self.assertEqual(
            dict(conf),
            {'config_path': '/usr/local/airflow/config/utils',
             'dag_email': 'test@domain.com',
             'dag_email_on_failure': 'False',
             'dag_email_on_retry': 'False',
             'data_path': '/usr/local/airflow/data/utils',
             'email_path': '/usr/local/airflow/emails/utils',
             'environment': 'DEV',
             'sql_path': '/usr/local/airflow/sql/utils',
             'test_path1': '/usr/local/airflow/data/utils/dir_dev1',
             'test_path_dev2': '/usr/local/airflow/data/utils/dir_dev2',
             'var1': 'value1'})

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_make_data_dir(self, mock_get, mock_file, mock_os_makedirs):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        configuration.get_config(config_filename='config.ini',
                                 make_data_dir=False)

        mock_os_makedirs.assert_not_called()

    @patch('os.makedirs')
    @patch('builtins.open')
    @patch('.utils.configuration.Variable.get')
    def test_get_config_default_config_file(self, mock_get, mock_file, _):
        mock_get.side_effect = self.config_vars.get
        mock_file.return_value = io.StringIO(self.config_ini_missing_section)
        configuration.get_config()

        mock_file.assert_called_with('/usr/local/airflow/config/utils/config.ini', encoding=None)


if __name__ == '__main__':
    unittest.main()
