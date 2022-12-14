# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import re
from typing import Any, List, Sequence

import backoff
import polars as pl
import pyarrow as pa
from lib_airflow.hooks.db.connectorx import ConnectorXHook

from .upload_to_bq import UploadToBigQueryOperator


class QueryAndUploadToBigQueryOperator(UploadToBigQueryOperator):
    """Does a ConnectorX query into a Polars DataFrame, store the DataFrame in a temporary Parquet file,
    upload the Parquet file to Cloud Storage, and load it into BigQuery."""

    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        connectorx_source_db_conn_id: str,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            connectorx_source_db_conn_id (str): The ConnectorX connection id
        """
        super().__init__(**kwargs)
        self.connectorx_source_db_conn_id = connectorx_source_db_conn_id

        self._db_hook = None
        self._re_pattern_bigquery_safe_column_names = re.compile(r"[^_0-9a-zA-Z]+")

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
        df = hook.get_polars_dataframe(query=sql)
        return df

    def _get_db_hook(self) -> ConnectorXHook:
        """Get the ConnectorX hook

        Returns:
            ConnectorXHook: The ConnectorX hook
        """
        if not self._db_hook:
            self._db_hook = ConnectorXHook(
                connectorx_conn_id=self.connectorx_source_db_conn_id
            )
        return self._db_hook

    def _check_dataframe_for_bigquery_safe_column_names(self, df: pl.DataFrame):
        column_names = df.columns
        safe_column_names = self._generate_bigquery_safe_column_names(column_names)

        for idx, safe_column_name in enumerate(safe_column_names):
            column_name = column_names[idx]
            if safe_column_name != column_name:
                df = df.with_column(df[column_name].alias(safe_column_name)).drop(
                    column_name
                )
        return df

    def _generate_bigquery_safe_column_names(
        self, column_names: List[str]
    ) -> List[str]:
        safe_column_names = column_names.copy()

        for idx, column_name in enumerate(column_names):
            # Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 300 characters long
            safe_column_name = self._re_pattern_bigquery_safe_column_names.sub(
                "_", column_name
            )
            if safe_column_name[0].isdigit():
                safe_column_name = f"_{safe_column_name}"
            while (
                safe_column_name
                in safe_column_names[:idx] + safe_column_names[idx + 1 :]
            ):
                safe_column_name = f"_{safe_column_name}"
            safe_column_name = safe_column_name[:300]

            if column_name != safe_column_name:
                safe_column_names[idx] = safe_column_name

        return safe_column_names
