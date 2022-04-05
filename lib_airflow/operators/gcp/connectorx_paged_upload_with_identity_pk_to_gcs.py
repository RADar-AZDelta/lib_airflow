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


class ConnectorXPagedUploadWithIdentityPkToGCSOperator(ConnectorXToGCSOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "impersonation_chain",
        "bucket_dir",
        "start_identity",
        "page_size",
        "table",
        "identity_column",
        "sql",
        "max_identity_sql",
    )
    ui_color = "#e0a98c"

    def __init__(
        self,
        *,
        table: str,
        identity_column: str,
        sql: str = """{% raw %}
select *
from {{ table }}
where {{ identity_column }} >= {{ current_identity_value_lower }} and {{ identity_column }} < {{ current_identity_value_upper }}
{% endraw %}""",
        max_identity_sql: str = """{% raw %}
select max({{ identity_column }}) as max_identity_value
from {{ table }}
{% endraw %}""",
        start_identity: Union[int, str] = 0,
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        func_page_loaded: Optional[Callable[[str, Context, int], None]] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)

        self.table = table
        self.identity_column = identity_column
        self.max_identity_sql = max_identity_sql
        self.start_identity = start_identity
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.func_page_loaded = func_page_loaded

    def execute(self, context: "Context") -> None:
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.max_identity_sql)
        sql = template.render(
            table=self.table,
            identity_column=self.identity_column,
        )
        hook = self._get_connectorx_hook()
        table = hook.get_arrow_table(sql)
        max_identity_value = table["max_identity_value"][0].as_py()

        current_identity_value_lower = cast(int, self.start_identity)
        while current_identity_value_lower < (
            max_identity_value + cast(int, self.page_size)
        ):
            current_identity_value_upper = current_identity_value_lower + cast(
                int, self.page_size
            )
            template = jinja_env.from_string(self.sql)
            sql = template.render(
                table=self.table,
                identity_column=self.identity_column,
                current_identity_value_lower=current_identity_value_lower,
                current_identity_value_upper=current_identity_value_upper,
            )
            returned_rows = self._paged_upload(
                sql, current_identity_value_lower // cast(int, self.page_size)
            )
            current_identity_value_lower += cast(int, self.page_size)

            if self.func_page_loaded:
                self.func_page_loaded(
                    self.table,
                    context,
                    current_identity_value_lower,
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
