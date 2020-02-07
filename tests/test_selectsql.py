import unittest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from selectsql import migrate_params, render
from cjwmodule.testing.i18n import i18n_message


class MigrateParamsTest(unittest.TestCase):
    def test_v0(self):
        self.assertEqual(
            migrate_params({"run": "", "sql": "SELECT * FROM input"}),
            {"sql": "SELECT * FROM input"},
        )

    def test_v1(self):
        self.assertEqual(
            migrate_params({"sql": "SELECT * FROM input"}),
            {"sql": "SELECT * FROM input"},
        )


class RenderTest(unittest.TestCase):
    def test_happy_path(self):
        df = DataFrame({"foo": [1, 2, 3], "bar": [2, 3, 4]})
        result = render(df, {"sql": "SELECT foo + bar AS baz FROM input"})
        expected = (DataFrame({"baz": [3, 5, 7]}), "")

        self.assertEqual(result[1], expected[1])
        self.assertTrue(result[0].equals(expected[0]))

    def test_empty_sql(self):
        df = DataFrame({"foo": [1, 2, 3]})
        result = render(df, {"sql": ""})
        self.assertIsNone(result[0])
        self.assertEqual(result[1], i18n_message("badParam.sql.missing"))

    def test_invalid_sql_syntax(self):
        df = DataFrame({"foo": [1, 2, 3]})
        result = render(df, {"sql": "This is not SQL"})
        self.assertIsNone(result[0])
        self.assertEqual(result[1], ['SQL error near "This": syntax error'])

    def test_hint_invalid_table_name(self):
        df = DataFrame({"foo": [1, 2, 3]})
        result = render(df, {"sql": "SELECT * FROM input2"})
        self.assertIsNone(result[0])
        self.assertEqual(
            result[1],
            [
                "SQL error: no such table: input2",
                i18n_message(
                    "badValue.sql.invalidTableName",
                    {"table_name": "input"}
                )
            ],
        )

    def test_empty_input(self):
        df = DataFrame()
        result = render(df, {"sql": "SELECT * FROM input"})
        assert_frame_equal(result[0], DataFrame())


if __name__ == "__main__":
    unittest.main()
