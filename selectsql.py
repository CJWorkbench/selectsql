import sqlite3
import pandas
from pandas.io.sql import DatabaseError


def sqlselect(table, sql):
    if len(table.columns) == 0:
        return (pandas.DataFrame(), "")

    with sqlite3.connect(":memory:") as conn:
        table.to_sql("input", conn, index=False)

        try:
            dataframe = pandas.read_sql_query(sql, conn)
        except DatabaseError as err:
            message = str(err)

            expect_start = f"Execution failed on sql '{sql}': "
            if message.startswith(expect_start):
                message = message.replace(expect_start, "SQL error: ", 1)

            if message.startswith("SQL error: near "):
                message = message.replace("SQL error: near ", "SQL error near ", 1)

            if "no such table: " in message:
                message += '\n\nThe only valid table name is "input"'

            return (None, message)

        return (dataframe, "")


def render(table, params):
    sql = params["sql"]
    if not sql.strip():
        return (None, "Missing SQL SELECT statement")

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
