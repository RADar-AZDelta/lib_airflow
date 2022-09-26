# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""MsSQL using pyodbc to GCS operator."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict

import pyarrow as pa
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook


class MSSQLOdbcToGCSOperator(BaseSQLToGCSOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON or CSV format using OdbcHook instead of MsSqlHook.
    :param odbc_conn_id: Reference to a specific ODBC hook.
    :type odbc_conn_id: str
    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::
            export_customers = MSSQLOdbcToGCSOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                odbc_conn_id='odbc_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    ui_color = "#e0a98c"

    """
    see https://docs.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver15
    and https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#bytes_type
    """
    type_map = {
        float: "FLOAT",
        bytes: "BYTES",
        bool: "BOOL",
        str: "STRING",
        datetime: "DATETIME",
        date: "DATETIME",
        int: "INTEGER",
        bytearray: "BYTES",
        Decimal: "NUMERIC",
    }

    def __init__(self, *, odbc_conn_id="odbc_conn_id", **kwargs) -> None:
        """Constructor

        Args:
            odbc_conn_id (str, optional): The id of the connection. Defaults to "odbc_conn_id".
        """
        super().__init__(**kwargs)
        self.odbc_conn_id = odbc_conn_id

    def get_db_conn(self) -> Any:
        """Get the database connection object.

        Returns:
            Any: PyODBC connection object
        """
        self.log.info("Starting ODBC hook with connection id '%s'", self.odbc_conn_id)
        mssqlodbc = OdbcHook(odbc_conn_id=self.odbc_conn_id)
        conn = mssqlodbc.get_conn()
        return conn

    def convert_types(self, schema, col_type_dict, row) -> list:
        """Convert values from DBAPI to output-friendly formats."""
        return [
            self.convert_type(value, col_type_dict.get(name), name, row)
            for name, value in zip(schema, row)
        ]

    def query(self) -> Any:
        """Queries MSSQL and returns a cursor of results.

        Returns:
            Any: PyODBC cursor
        """
        self.log.info("Executing query: %s", self.sql.strip())
        conn = self.get_db_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql.strip())
        return cursor

    def field_to_bigquery(self, field) -> Dict[str, str]:
        """
        see https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        return {
            "name": field[0].replace(" ", "_"),
            "type": self.type_map.get(field[1], "STRING"),
            "mode": "NULLABLE" if field[6] else None,
        }

    def convert_type(self, value, schema_type, name, row):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        Converted from classmethod to a normal mathod!
        """
        if isinstance(value, Decimal):
            return float(value)
        return value
