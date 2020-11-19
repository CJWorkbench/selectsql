import datetime

from cjwmodule.testing.i18n import i18n_message
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from selectsql import migrate_params, render


def test_migrate_params_v0():
    assert migrate_params({"run": "", "sql": "SELECT * FROM input"}) == {
        "sql": "SELECT * FROM input"
    }


def test_migrate_params_v1():
    assert migrate_params({"sql": "SELECT * FROM input"}) == {
        "sql": "SELECT * FROM input"
    }


def test_render_happy_path():
    df = DataFrame({"foo": [1, 2, 3], "bar": [2, 3, 4]})
    result = render(df, {"sql": "SELECT foo + bar AS baz FROM input"})
    assert result[1] == []
    assert_frame_equal(result[0], DataFrame({"baz": [3, 5, 7]}), [])


def test_render_datetime_roundtrip():
    dt1 = datetime.datetime(2020, 6, 17, 18, 38, 1)
    dt2 = datetime.datetime(2020, 6, 17, 18, 41, 1)
    df = DataFrame({"dt": [dt1, dt2]})
    result = render(df, {"sql": "SELECT dt FROM input"})
    expected = (DataFrame({"dt": [dt1, dt2]}), "")

    assert result[1] == []
    assert_frame_equal(result[0], expected[0])


def test_render_str_roundtrip():
    df = DataFrame({"A": ["b", "c", "dé"]})
    result = render(df, {"sql": "SELECT * FROM input"})
    assert result[1] == []
    assert_frame_equal(result[0], df)


def test_render_empty_sql():
    df = DataFrame({"foo": [1, 2, 3]})
    result = render(df, {"sql": ""})
    assert result == (None, [i18n_message("badParam.sql.missing")])


def test_render_commented_sql():
    df = DataFrame({"foo": [1, 2, 3]})
    result = render(df, {"sql": "-- SELECT * FROM input"})
    assert result == (None, [i18n_message("badValue.sql.commentedQuery")])


def test_render_invalid_sql_syntax():
    df = DataFrame({"foo": [1, 2, 3]})
    result = render(df, {"sql": "This is not SQL"})
    assert result == (None, ['SQL error near "This": syntax error'])


def test_render_hint_invalid_table_name():
    df = DataFrame({"foo": [1, 2, 3]})
    result = render(df, {"sql": "SELECT * FROM input2"})
    assert result == (
        None,
        [i18n_message("badValue.sql.invalidTableName", {"table_name": "input"})],
    )


def test_render_empty_input():
    df = DataFrame()
    result = render(df, {"sql": "SELECT * FROM input"})
    assert result[1] == []
    assert_frame_equal(result[0], DataFrame())


def test_render_duplicate_column_name():
    df = DataFrame({"A": [1, 2, 3]})
    result = render(df, {"sql": "SELECT A, A FROM input"})
    assert result == (
        None,
        [
            i18n_message("badValue.sql.duplicateColumnName", {"colname": "A"}),
        ],
    )
