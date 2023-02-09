# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import re
from typing import Any, List, Sequence, cast

import backoff
import polars as pl
import pyarrow as pa

from ...hooks.db import BigQueryHook, ConnectorXHook
from .upload_to_bq import UploadToBigQueryOperator


class QueryAndUploadToBigQueryOperator(UploadToBigQueryOperator):
    """Does a ConnectorX query into a Polars DataFrame, store the DataFrame in a temporary Parquet file,
    upload the Parquet file to Cloud Storage, and load it into BigQuery."""

    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        source_db_conn_id: str,
        db_hook_type: str = "ConnectorX",
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            source_db_conn_id (str): The ConnectorX connection id
        """
        super().__init__(**kwargs)
        self.source_db_conn_id = source_db_conn_id
        self._db_hook_type = db_hook_type

        self._db_hook = None

    def _query_to_parquet_and_upload(
        self,
        sql: str,
        object_name: str,
        table_metadata: Any = None,
    ) -> pl.DataFrame | pa.Table:
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
        df = self._check_dataframe_for_bigquery_safe_column_names(df)
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
        self.log.debug("Running query: %s", sql)
        hook = self._get_db_hook()
        df = cast(pl.DataFrame, hook.run(sql))
        return df

    def _get_db_hook(self) -> ConnectorXHook | BigQueryHook:
        """Get the ConnectorX hook

        Returns:
            ConnectorXHook: The ConnectorX hook
        """
        if not self._db_hook:
            if self._db_hook_type == "ConnectorX":
                self._db_hook = ConnectorXHook(
                    connectorx_conn_id=self.source_db_conn_id
                )
            elif self._db_hook_type == "BigQuery":
                self._db_hook = BigQueryHook(
                    conn_id=self.source_db_conn_id,
                    location=self.gcp_location,
                )
            else:
                raise Exception(f"Unknown db hook type '{self._db_hook_type}'")
        return self._db_hook
