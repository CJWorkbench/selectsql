import datetime
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
        self.assertEqual(result[1], [])
        assert_frame_equal(result[0], DataFrame({"baz": [3, 5, 7]}), [])

    def test_datetime_roundtrip(self):
        dt1 = datetime.datetime(2020, 6, 17, 18, 38, 1)
        dt2 = datetime.datetime(2020, 6, 17, 18, 41, 1)
        df = DataFrame({"dt": [dt1, dt2]})
        result = render(df, {"sql": "SELECT dt FROM input"})
        expected = (DataFrame({"dt": [dt1, dt2]}), "")

        self.assertEqual(result[1], [])
        assert_frame_equal(result[0], expected[0])

    def test_str_roundtrip(self):
        df = DataFrame({"A": ["b", "c", "dé"]})
        result = render(df, {"sql": "SELECT * FROM input"})
        self.assertEqual(result[1], [])
        assert_frame_equal(result[0], df)

    def test_empty_sql(self):
        df = DataFrame({"foo": [1, 2, 3]})
        result = render(df, {"sql": ""})
        self.assertIsNone(result[0])
        self.assertEqual(result[1], [i18n_message("badParam.sql.missing")])

    def test_commented_sql(self):
        df = DataFrame({"foo": [1, 2, 3]})
        result = render(df, {"sql": "-- SELECT * FROM input"})
        self.assertIsNone(result[0])
        self.assertEqual(result[1], [i18n_message("badValue.sql.commentedQuery")])

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
            [i18n_message("badValue.sql.invalidTableName", {"table_name": "input"})],
        )

    def test_empty_input(self):
        df = DataFrame()
        result = render(df, {"sql": "SELECT * FROM input"})
        assert_frame_equal(result[0], DataFrame())

    def test_duplicate_column_name(self):
        df = DataFrame({"A": [1, 2, 3]})
        result = render(df, {"sql": "SELECT A, A FROM input"})
        self.assertIsNone(result[0])
        self.assertEqual(
            result[1],
            [i18n_message("badValue.sql.duplicateColumnName", {"colname": "A"}),],
        )


if __name__ == "__main__":
    unittest.main()
