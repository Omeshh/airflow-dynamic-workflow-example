import unittest
from unittest.mock import patch, call
from common.utils import os_utils


class TestOSUtils(unittest.TestCase):
    def setUp(self):
        self.paths = ['/usr/local/airflow/data/utils/dir_dev1',
                      '/usr/local/airflow/data/utils/dir_dev2']

    @patch('os.makedirs')
    def test_make_paths(self, mock_os_makedirs):
        os_utils.make_paths(self.paths)

        self.assertEqual(
            mock_os_makedirs.mock_calls,
            [call(self.paths[0], exist_ok=True),
             call(self.paths[1], exist_ok=True)])

    @patch('os.makedirs')
    def test_make_paths_str_paths(self, mock_os_makedirs):
        os_utils.make_paths(self.paths[0])

        self.assertEqual(
            mock_os_makedirs.mock_calls,
            [call(self.paths[0], exist_ok=True)])

    @patch('os.makedirs')
    def test_make_paths_exist_ok_false(self, mock_os_makedirs):
        os_utils.make_paths(self.paths, exist_ok=False)

        self.assertEqual(
            mock_os_makedirs.mock_calls,
            [call(self.paths[0], exist_ok=False),
             call(self.paths[1], exist_ok=False)])


if __name__ == '__main__':
    unittest.main()
