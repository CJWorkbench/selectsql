import sqlite3
import pandas
from pandas.io.sql import DatabaseError
from cjwmodule import i18n


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

            messages = [message]
            if "no such table: " in message:
                messages.append(
                    i18n.trans(
                        "badValue.sql.invalidTableName",
                        'The only valid table name is "{table_name}"',
                        {"table_name": "input"},
                    )
                )

            return (None, messages)

        duplicated = dataframe.columns[dataframe.columns.duplicated()]
        if len(duplicated):
            return (
                None,
                [
                    i18n.trans(
                        "badValue.sql.duplicateColumnName",
                        'You selected two columns named "{column_name}". Please delete one or alias it with "AS".',
                        {"column_name": duplicated[0]},
                    )
                ],
            )

        return (dataframe, "")


def render(table, params):
    sql = params["sql"]
    if not sql.strip():
        return (
            None,
            i18n.trans("badParam.sql.missing", "Missing SQL SELECT statement"),
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
