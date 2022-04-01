"""ConnectorX to GCS operator."""

import time
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Callable, List, Optional, Sequence, Tuple, Union, cast

import backoff
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from lib_airflow.hooks.db.connectorx import ConnectorXHook


class ConnectorXPagedUploadToGCSOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "impersonation_chain",
        "bucket_dir",
        "start_page",
        "page_size",
        "table",
    )
    ui_color = "#e0a98c"

    def __init__(
        self,
        *,
        connectorx_conn_id: str,
        table: str,
        pk_columns: List[str],
        sql: str = """select *
from {{ table }}
ORDER BY {{ order_by }}
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY""",
        start_page: Union[int, str] = 0,
        page_size: Union[int, str] = 100000,
        bucket: str,
        bucket_dir: str = "upload",
        gzip: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        func_page_loaded: Callable[[str, int], None] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.table = table
        self.pk_columns = pk_columns
        self.sql = sql
        self.start_page = start_page
        self.page_size = page_size
        self.bucket = bucket
        self.bucket_dir = bucket_dir
        self.gzip = gzip
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.func_page_loaded = func_page_loaded

        self.file_mime_type = "application/octet-stream"
        self.gcs_hook = None
        self.connectorx_hook = None

    def execute(self, context: "Context") -> None:
        jinja_env = self.get_template_env()
        order_by = ", ".join(self.pk_columns)

        page = cast(int, self.start_page)
        returned_rows = self.page_size
        while returned_rows == self.page_size:
            template = jinja_env.from_string(self.sql)
            sql = template.render(
                table=self.table,
                order_by=order_by,
                page_size=self.page_size,
                page=page,
            )
            returned_rows = self._paged_upload(sql, page)
            page += 1
            if self.func_page_loaded:
                self.func_page_loaded(self.table, page)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _paged_upload(self, sql: str, page: int) -> int:
        table = self._query(sql)
        returned_rows = table.num_rows
        self.log.info(f"Rows fetched for page {page}: {returned_rows}")

        with self._write_local_data_files(table) as file_to_upload:
            file_to_upload.flush()
            self._upload_to_gcs(
                file_to_upload, f"{self.bucket_dir}/{self.table}_{page}.parquet"
            )
        return returned_rows

    def _query(self, sql) -> pa.Table:
        self.log.info("Running query '%s", sql)
        hook = self._get_connectorx_hook()
        table = hook.get_arrow_table(sql)
        return table

    def _write_local_data_files(self, table: pa.Table) -> _TemporaryFileWrapper:
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)
        pq.write_table(table, tmp_file_handle.name)
        return tmp_file_handle

    def _upload_to_gcs(self, file_to_upload, filename):
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = self._get_gcs_hook_hook()
        hook.upload(
            self.bucket,
            filename,
            file_to_upload.name,
            mime_type=self.file_mime_type,
            gzip=self.gzip,
        )

    def _get_connectorx_hook(self):
        if not self.connectorx_hook:
            self.connectorx_hook = ConnectorXHook(self.connectorx_conn_id)
        return self.connectorx_hook

    def _get_gcs_hook_hook(self):
        if not self.gcs_hook:
            self.gcs_hook = GCSHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        return self.gcs_hook
