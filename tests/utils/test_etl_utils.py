import unittest
from common.utils import etl_utils
import pandas as pd


class TestETLUtils(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame([{"AccountNumber": "     12345    ",
                                 "Billingcycle": 6,
                                 "FirstName": "Adam",
                                 "LastName": "Hyatt",
                                 "Address": "Redding, CA 96001"},
                                {"AccountNumber": "",
                                 "Billingcycle": 12,
                                 "FirstName": "Charles",
                                 "LastName": "Smith",
                                 "Address": "Boston, MA 02130 USA"},
                                {"AccountNumber": "     1122    ",
                                 "Billingcycle": 18,
                                 "FirstName": "Cristal",
                                 "LastName": "",
                                 "Address": ""}],
                               columns=['AccountNumber', 'Cycle', 'FirstName', 'LastName', 'Address'])

    def tearDown(self):
        self.df = None

    def test_apply_transformations(self):
        self.assertEqual(list(etl_utils.apply_transformations(
            df=self.df,
            transformations={
                "RENAME:Cycle": "RenamedBillingCycle",
                "ID": 1234,
                "AccountNumber": lambda row: row['AccountNumber'].strip(),
                "CustomerName": lambda row: "{} {}".format(row["FirstName"].strip(), row["LastName"].strip()[:100]),
                "Address": lambda row: row["Address"].encode("utf-16le"),
                "FILTER:CheckEmpty": lambda row: row['AccountNumber'].strip() != ""})),
            [{'AccountNumber': '12345',
              'Address': b'1\x006\x005\x007\x00 \x00R\x00i\x00v\x00e\x00r\x00s\x00i\x00'
                         b'd\x00e\x00 \x00D\x00r\x00i\x00v\x00e\x00 \x00R\x00e\x00d\x00'
                         b'd\x00i\x00n\x00g\x00,\x00 \x00C\x00A\x00 \x009\x006\x000\x00'
                         b'0\x001\x00',
              'Cycle': 6,
              'FirstName': 'Adam',
              'LastName': 'Smith',
              'CustomerName': 'Adam Smith',
              'ID': 1234},
             {'AccountNumber': '1122',
              'Address': b'',
              'RenamedBillingCycle': 18,
              'FirstName': 'Cristal',
              'LastName': '',
              'CustomerName': 'Cristal ',
              'ID': 1234}])

    def test_apply_transformations_empty_df(self):
        self.assertEqual(list(etl_utils.apply_transformations(
            df=pd.DataFrame(columns=['AccountNumber', 'Billingcycle', 'FirstName', 'LastName', 'Address']),
            transformations={
                "RENAME:Cycle": "RenamedBillingCycle",
                "ID": '1234',
                "AccountNumber": lambda row: row['AccountNumber'].strip(),
                "CustomerName": lambda row: "{} {}".format(row["FirstName"].strip(), row["LastName"].strip()[:100]),
                "Address": lambda row: row["Address"].encode("utf-16le"),
                "FILTER:CheckEmpty": lambda row: row['AccountNumber'].strip() != ""})),
            [])

    def test_apply_transformations_empty_transformations(self):
        self.assertEqual(list(etl_utils.apply_transformations(
            df=self.df,
            transformations={})),
            self.df.to_dict('records'))


if __name__ == '__main__':
    unittest.main()
