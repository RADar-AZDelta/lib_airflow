# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""ConnectorX to GCS operator."""

from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Callable, Optional, Sequence, Union

import backoff
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from lib_airflow.hooks.db import ConnectorXHook


class ConnectorXToGCSOperator(BaseOperator):
    """This operator lets you do a SQL query (with ConnectorX) and upload the results as Parquet to Cloud Storage"""

    template_fields: Sequence[str] = (
        "sql",
        "bucket",
        "filename",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql", ".sql.jinja")
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#e0a98c"

    def __init__(
        self,
        *,
        connectorx_conn_id: str,
        sql: str,
        func_modify_data: Optional[
            Callable[[pl.DataFrame, Context], pl.DataFrame]
        ] = None,
        # schema: pa.Schema = None,
        bucket: str,
        filename: str = "upload.parquet",
        gzip: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            connectorx_conn_id (str): The ConnectorX connection ID
            sql (str): The SQL query
            bucket (str): The cloud Storage Bucket
            func_modify_data (Optional[ Callable[[pl.DataFrame, Context], pl.DataFrame] ], optional): This function lets you modify the results of the Query. With this function you can do for instance do pseudonimisation. Defaults to None.
            filename (str, optional): The name of the uploaded Parquet file. Defaults to "upload.parquet".
            gzip (bool, optional): Compress local file before upload (Parquet files are already zipped). Defaults to False.
            gcp_conn_id (str, optional): GCP Cloud Storage connection ID. Defaults to "google_cloud_default".
            delegate_to (Optional[str], optional): This performs a task on one host with reference to other hosts. Defaults to None.
            impersonation_chain (Optional[Union[str, Sequence[str]]], optional): This is the optional service account to impersonate using short term credentials. Defaults to None.
        """
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.sql = sql
        self.func_modify_data = func_modify_data
        # self.schema = schema
        self.bucket = bucket
        self.filename = filename
        self.gzip = gzip
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        self.file_mime_type = "application/octet-stream"
        self.gcs_hook = None
        self.connectorx_hook = None

    def execute(self, context: "Context") -> None:
        """Execute the operator (do the SQL query and upload the results as Parquet to Cloud Storage)

        Args:
            context (Context):  Jinja2 template context for task rendering.
        """
        table = self._query(self.sql, context)
        with self._write_local_data_files(table) as file_to_upload:
            file_to_upload.flush()
            self._upload_to_gcs(file_to_upload, self.filename)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _query(
        self, sql: str, context: "Context" = None
    ) -> Union[pa.Table, pl.DataFrame, pd.DataFrame]:
        """Execute a query and store the results into an Arrow table, Polars dataframe or Pandas dataframe

        Args:
            sql (str): The SQL query
            context (Context, optional): Jinja2 template context for task rendering. Defaults to None.

        Returns:
            Union[pa.Table, pl.DataFrame, pd.DataFrame]: Arrow table, Polars dataframe or Pandas dataframe
        """
        self.log.info("Running query '%s", sql)
        hook = self._get_connectorx_hook()

        if hook.connection.conn_type == "google_cloud_platform":
            df = hook.get_pandas_dataframe(sql)
            return df
        elif self.func_modify_data:
            df = hook.get_polars_dataframe(sql)
            return self.func_modify_data(df, context)
        else:
            table = hook.get_arrow_table(sql)
            return table

    def _write_local_data_files(
        self, data: Union[pa.Table, pl.DataFrame, pd.DataFrame]
    ) -> _TemporaryFileWrapper:
        """Write the Arrow table, Polars dataframe or Pandas dataframe to a temporary Parquet file

        Args:
            data (Union[pa.Table, pl.DataFrame, pd.DataFrame]): The Arrow table, Polars dataframe or Pandas dataframe

        Returns:
            _TemporaryFileWrapper: The temporary Parquet file
        """
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)
        if isinstance(data, pl.DataFrame):
            data = data.to_arrow()
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(
                data  # , self.schema, preserve_index=False, safe=False
            )
        pq.write_table(
            data,
            tmp_file_handle.name,
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )
        return tmp_file_handle

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _upload_to_gcs(self, file_to_upload: str, filename: str):
        """Upload file to Cloud Storage

        Args:
            file_to_upload (str): Name that the uploaded file will have in the bucket
            filename (str): File to upload
        """
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = self._get_gcs_hook()
        hook.upload(
            self.bucket,
            filename,
            file_to_upload.name,
            mime_type=self.file_mime_type,
            gzip=self.gzip,
        )

    def _get_connectorx_hook(self):
        """Get the ConnectorX hook

        Returns:
            ConnectorXHook: The ConnectorX hook
        """
        if not self.connectorx_hook:
            self.connectorx_hook = ConnectorXHook(self.connectorx_conn_id)
        return self.connectorx_hook

    def _get_gcs_hook(self):
        """Get the Cloud Storage Hook

        Returns:
            GCSHook: The Cloud Storage Hook
        """
        if not self.gcs_hook:
            self.gcs_hook = GCSHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        return self.gcs_hook
