"""ConnectorX to GCS operator."""

import time
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Optional, Sequence, Tuple, Union

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from lib_airflow.hooks.db.connectorx import ConnectorXHook


class ConnectorXToGCSOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "sql",
        "bucket",
        "filename",
        # "schema_filename",
        # "schema",
        # "parameters",
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
        bucket: str,
        filename: str = "upload.parquet",
        # schema_filename: Optional[str] = None,
        # approx_max_file_size_bytes: int = 1900000000,
        # null_marker: Optional[str] = None,
        gzip: bool = False,
        # schema: Optional[Union[str, list]] = None,
        # parameters: Optional[dict] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.gzip = gzip
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.file_mime_type = "application/octet-stream"

    def execute(self, context: "Context"):
        table = self._query()
        with self._write_local_data_files(table) as file_to_upload:
            file_to_upload.flush()
            self._upload_to_gcs(file_to_upload)

    def _query(self) -> pa.Table:
        self.log.info("Running query '%s", self.sql)
        hook = ConnectorXHook(self.connectorx_conn_id)
        table = hook.get_arrow_table(self.sql)
        return table

    def _write_local_data_files(self, table) -> _TemporaryFileWrapper:
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)
        pq.write_table(table, tmp_file_handle.name)
        return tmp_file_handle

    def _upload_to_gcs(self, file_to_upload):
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook.upload(
            self.bucket,
            self.filename,
            file_to_upload.name,
            mime_type=self.file_mime_type,
            gzip=self.gzip,
        )
