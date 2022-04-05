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
from airflow.utils.context import Context
from lib_airflow.hooks.db import ConnectorXHook
from lib_airflow.operators.gcp.connectorx_to_gcs import ConnectorXToGCSOperator


class ConnectorXPagedUploadToGCSOperator(ConnectorXToGCSOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "impersonation_chain",
        "bucket_dir",
        "start_page",
        "page_size",
        "table",
        "pk_columns",
        "sql",
    )
    ui_color = "#e0a98c"

    def __init__(
        self,
        *,
        table: str,
        pk_columns: Union[List[str], str],
        sql: str = """{% raw %}
select *
from {{ table }}
ORDER BY {{ order_by }}
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        start_page: Union[int, str] = 0,
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        func_page_loaded: Optional[Callable[[str, Context, int], None]] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)

        self.table = table
        self.pk_columns = pk_columns
        self.start_page = start_page
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.func_page_loaded = func_page_loaded

    def execute(self, context: "Context") -> None:
        jinja_env = self.get_template_env()
        order_by = ", ".join(self.pk_columns)

        page = cast(int, self.start_page) or 0
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
                self.func_page_loaded(self.table, context, page)

    def _paged_upload(self, sql: str, page: int) -> int:
        table = self._query(sql)
        returned_rows = table.num_rows
        self.log.info(f"Rows fetched for page {page}: {returned_rows}")

        if returned_rows > 0:
            with self._write_local_data_files(table) as file_to_upload:
                file_to_upload.flush()
                self._upload_to_gcs(
                    file_to_upload, f"{self.bucket_dir}/{self.table}_{page}.parquet"
                )
        return returned_rows
