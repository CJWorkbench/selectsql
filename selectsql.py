import contextlib
import sqlite3
from typing import List, Union

import pandas as pd
from cjwmodule import i18n


def _database_error_to_messages(
    err: sqlite3.DatabaseError,
) -> List[Union[i18n.I18nMessage, str]]:
    if isinstance(err, sqlite3.OperationalError) and err.args[0].startswith(
        "no such table: "
    ):
        return [
            i18n.trans(
                "badValue.sql.invalidTableName",
                'The only valid table name is "{table_name}"',
                {"table_name": "input"},
            )
        ]

    if err.args[0].startswith("near "):
        return [f"SQL error {str(err)}"]  # it's English anyway

    return [str(err)]  # it's English anyway


@contextlib.contextmanager
def _deleting_cursor(c):
    try:
        yield c
    finally:
        c.close()


def sqlselect(table: pd.DataFrame, sql):
    if len(table.columns) == 0:
        return (pd.DataFrame(), "")

    with sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES) as conn:
        table.to_sql("input", conn, index=False)

        with _deleting_cursor(conn.cursor()) as c:
            try:
                c.execute(sql)
            except sqlite3.DatabaseError as err:
                return None, _database_error_to_messages(err)

            if c.description is None:
                return (
                    None,
                    [
                        i18n.trans(
                            "badValue.sql.commentedQuery",
                            "Your query did nothing. Did you accidentally comment it out?",
                        )
                    ],
                )

            colnames = [d[0] for d in c.description]

            dupdetect = set()
            for colname in colnames:
                if colname in dupdetect:
                    return (
                        None,
                        [
                            i18n.trans(
                                "badValue.sql.duplicateColumnName",
                                'Your query would produce two columns named {colname}. Please delete one or alias it with "AS".',
                                {"colname": colname},
                            )
                        ],
                    )
                dupdetect.add(colname)

            # Memory-inefficient: creates a Python object per value
            data = c.fetchall()  # TODO benchmark c.arraysize=1000, =100000, etc.

    return pd.DataFrame.from_records(data, columns=colnames), []


def render(table, params):
    sql = params["sql"]
    if not sql.strip():
        return (
            None,
            [i18n.trans("badParam.sql.missing", "Missing SQL SELECT statement")],
        )

    return sqlselect(table, sql)


def _migrate_params_v0_to_v1(params):
    """
    v0 had an empty 'run' param (it was a value generated from a button).

    v1 has no 'run' param.
    """
    return {"sql": params["sql"]}


def migrate_params(params):
    if "run" in params:
        params = _migrate_params_v0_to_v1(params)
    return params
