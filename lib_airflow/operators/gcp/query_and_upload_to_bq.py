# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Any, Sequence

import backoff
import polars as pl
from lib_airflow.hooks.db.connectorx import ConnectorXHook

from .upload_to_bq import UploadToBigQueryOperator


class QueryAndUploadToBigQueryOperator(UploadToBigQueryOperator):
    """Does a ConnectorX query into a Polars DataFrame, store the DataFrame in a temporary Parquet file,
    upload the Parquet file to Cloud Storage, and load it into BigQuery."""

    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        connectorx_db_conn_id: str,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            connectorx_db_conn_id (str): The ConnectorX connection id
        """
        super().__init__(**kwargs)
        self.connectorx_db_conn_id = connectorx_db_conn_id

        self._db_hook = None

    def _query_to_parquet_and_upload(
        self,
        sql: str,
        object_name: str,
        table_metadata: Any = None,
    ) -> int:
        """Do a query into a Polars DataFrame, store the DataFrame in a temporary Parquet file, upload the Parquet file to
        Cloud Storage, and load it into BigQuery.

        Args:
            sql (str): The SQL query
            object_name (str): The file name of the uploaded Parquet file in the Cloud starage bucket
            table_metadata (Any, optional): _description_. Defaults to None.

        Returns:
            int: Number of uploaded rows
        """
        df = self._query(sql)
        return self._upload_parquet(df, object_name, table_metadata)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _query(
        self,
        sql: str,
    ) -> pl.DataFrame:
        """Do a query into a Polars temporary Parquet file

        Args:
            sql (str): The SQL query

        Returns:
            pl.DataFrame: The polars DataFrame holding the query results
        """
        hook = self._get_db_hook()
        df = hook.get_polars_dataframe(query=sql)
        return df

    def _get_db_hook(self) -> ConnectorXHook:
        """Get the ConnectorX hook

        Returns:
            ConnectorXHook: The ConnectorX hook
        """
        if not self._db_hook:
            self._db_hook = ConnectorXHook(
                connectorx_conn_id=self.connectorx_db_conn_id
            )
        return self._db_hook
