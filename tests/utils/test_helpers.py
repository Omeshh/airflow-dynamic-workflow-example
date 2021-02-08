import unittest
from common.utils import helpers


class TestHelpers(unittest.TestCase):
    def test_render_template_int(self):
        self.assertEqual(helpers.render_template(
            attr=123,
            content={"test_key1": "test_value1", "test_key2": "test_value2"}),
            123)

    def test_render_template_str(self):
        self.assertEqual(helpers.render_template(
            attr="test_value",
            content={"test_key1": "test_value1", "test_key2": "test_value2"}),
            "test_value")

        self.assertEqual(helpers.render_template(
            attr="{{ test_attr1 }}",
            content={"test_attr1": "test_value1", "test_attr2": "test_value2"}),
            "test_value1")

        self.assertEqual(helpers.render_template(
            attr="Testing render for {{ test_attr2 }} string",
            content={"test_attr1": "test_value1", "test_attr2": "test_value2"}),
            "Testing render for test_value2 string")

    def test_render_template_list(self):
        self.assertEqual(helpers.render_template(
            attr=["abc", 123, {"k": "v"}],
            content={"test_key1": "test_value1", "test_key2": "test_value2"}),
            ["abc", 123, {"k": "v"}])

        self.assertEqual(helpers.render_template(
            attr=["{{ test_attr1 }}", "{{ test_attr2 }}", {"k": "v"}],
            content={"test_attr1": "test_value1", "test_attr2": "1122"}),
            ["test_value1", "1122", {"k": "v"}])

    def test_render_template_tuple(self):
        self.assertEqual(helpers.render_template(
            attr=("abc", 123, {"k": "v"}),
            content={"test_key1": "test_value1", "test_key2": "test_value2"}),
            ["abc", 123, {"k": "v"}])

        self.assertEqual(helpers.render_template(
            attr=("{{ test_attr1 }}", "{{ test_attr2 }}", {"k": "v"}),
            content={"test_attr1": "test_value1", "test_attr2": "1122"}),
            ["test_value1", "1122", {"k": "v"}])

    def test_render_template_dict(self):
        self.assertEqual(helpers.render_template(
            attr={"test_key1": "{{ test_attr1 }}", "test_key2": "{{ test_attr2 }}", "test_attr3": 123},
            content={"test_attr1": "test_value1", "test_attr2": "1122"}),
            {"test_key1": "test_value1", "test_key2": "1122", "test_attr3": 123})

        self.assertEqual(helpers.render_template(
            attr={"test_key1": "{{ test_attr1 }}", "test_key2": [1, 2, 3]},
            content={"test_attr1": "test_value1", "test_attr2": "1122"}),
            {"test_key1": "test_value1", "test_key2": [1, 2, 3]})

    def test_dict_merge(self):
        dct = {"test_key1": "test_value1",
               "test_key2": "test_value2",
               "test_key3": {"test_key3_1": "test_value3_1",
                             "test_key3_2": "test_value3_2",
                             "test_key3_3": [1, 2, 3],
                             "test_key3_4": [1, 2, 3],
                             "test_key3_5": 1234,
                             "test_key3_6": 1234,
                             "test_key3_7": {"test_key3_7_1": "test_value23_7_1",
                                             "test_key3_7_2": "test_value23_7_2",
                                             "test_key3_7_3": [1, 2, 3, 4, 5]}},
               "test_key4": ["list_item1", "list_item2", 1122],
               "test_key5": ["list_item1", "list_item2", 12345],
               "test_key6": 112233,
               "test_key7": 12345}

        merge_dct = {"test_key2": "test_value_merge",
                     "test_key3": {"test_key3_2": "test_value3_2_merge",
                                   "test_key3_4": [3, 2, 1],
                                   "test_key3_6": 4321,
                                   "test_key3_7": {"test_key3_7_2": "test_value3_7_2_merge",
                                                   "test_key3_7_3": [5, 4, 3, 2, 1],
                                                   "test_key3_7_4": "test_value3_7_4_merge"}},
                     "test_key5": ["list_item1_merge", 54321],
                     "test_key7": 54321,
                     "test_key8": {"test_key1_merge": "test_value3_2_merge",
                                   "test_key2_merge": ["test_value1", 12345],
                                   "test_key3_merge": {"test_key3_1_merge": "test_value3_1_merge",
                                                       "test_key3_2_merge": [5, 4, 3, 2, 1]}},
                     "test_key9": [1, 2, 3],
                     "test_key10": "test_value8"}

        helpers.dict_merge(
            dct=dct,
            merge_dct=merge_dct)

        self.assertEqual(dct,
                         {"test_key1": "test_value1",
                          "test_key2": "test_value_merge",
                          "test_key3": {"test_key3_1": "test_value3_1",
                                        "test_key3_2": "test_value3_2_merge",
                                        "test_key3_3": [1, 2, 3],
                                        "test_key3_4": [3, 2, 1],
                                        "test_key3_5": 1234,
                                        "test_key3_6": 4321,
                                        "test_key3_7": {"test_key3_7_1": "test_value23_7_1",
                                                        "test_key3_7_2": "test_value3_7_2_merge",
                                                        "test_key3_7_3": [5, 4, 3, 2, 1],
                                                        "test_key3_7_4": "test_value3_7_4_merge"}},
                          "test_key4": ["list_item1", "list_item2", 1122],
                          "test_key5": ["list_item1_merge", 54321],
                          "test_key6": 112233,
                          "test_key7": 54321,
                          "test_key8": {"test_key1_merge": "test_value3_2_merge",
                                        "test_key2_merge": ["test_value1", 12345],
                                        "test_key3_merge": {"test_key3_1_merge": "test_value3_1_merge",
                                                            "test_key3_2_merge": [5, 4, 3, 2, 1]}},
                          "test_key9": [1, 2, 3],
                          "test_key10": "test_value8"
                          })


if __name__ == '__main__':
    unittest.main()
