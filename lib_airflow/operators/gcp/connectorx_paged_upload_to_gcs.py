# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""ConnectorX to GCS operator."""

from typing import Callable, List, Optional, Sequence, Union, cast

import pandas as pd
import polars as pl
from airflow.utils.context import Context

from .connectorx_to_gcs import ConnectorXToGCSOperator


class ConnectorXPagedUploadToGCSOperator(ConnectorXToGCSOperator):
    """Do a paged upload of a table to Cloud Storage."""

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
        pk_columns: Union[List[str], str] = None,
        sql: str = """{% raw %}
{% if pk_columns %}
with cte as (
	select {{ pk_columns }}
	from {{ table }}
	ORDER BY {{ order_by }}
    OFFSET {{ page * page_size }} ROWS
    FETCH NEXT {{ page_size }} ROWS ONLY
)
select t.*
from cte 
inner join {{ table }} t on {{ join_clause }}
{% else %}
select *
from {{ table }} 
ORDER BY {{ order_by }}
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endif %}
{% endraw %}""",
        start_page: Union[int, str] = 0,
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        func_page_loaded: Optional[Callable[[str, Context, int], None]] = None,
        **kwargs,
    ) -> None:
        """_summary_

        Args:
            table (str): The name of the database table
            pk_columns (Union[List[str], str], optional): List of the primary key columns. Defaults to None.
            sql (str, optional): The paged query.
            start_page (Union[int, str], optional): Page to start from. Defaults to 0.
            page_size (Union[int, str], optional): Number of rows to fetch in each page. Defaults to 100000.
            bucket_dir (str, optional): The bucket dir to store the paged upload Parquet files. Defaults to "upload".
            func_page_loaded (Optional[Callable[[str, Context, int], None]], optional): Function that can be called every time a page is uploaded. Defaults to None.
        """
        super().__init__(sql=sql, **kwargs)

        self.table = table
        self.pk_columns = pk_columns
        self.start_page = start_page
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.func_page_loaded = func_page_loaded

    def execute(self, context: "Context") -> None:
        """Execute the operator.
        Do the paged SQL query and upload the results as Parquet to Cloud Storage.

        Args:
            context (Context):  Jinja2 template context for task rendering.
        """
        jinja_env = self.get_template_env()
        pk_columns = ", ".join(self.pk_columns) if self.pk_columns else None
        order_by = ", ".join(self.pk_columns) if self.pk_columns else "1"

        join_clause = (
            " and ".join(
                map(
                    lambda column: f"cte.[{column}] = t.[{column}]",
                    self.pk_columns,
                )
            )
            if self.pk_columns
            else None
        )
        page = cast(int, self.start_page) or 0
        returned_rows = self.page_size
        while returned_rows == self.page_size:
            template = jinja_env.from_string(self.sql)
            sql = template.render(
                table=self.table,
                pk_columns=pk_columns,
                order_by=order_by,
                join_clause=join_clause,
                page_size=self.page_size,
                page=page,
            )
            returned_rows = self._paged_upload(sql, page, context)
            page += 1
            if self.func_page_loaded:
                self.func_page_loaded(self.table, context, page)

    def _paged_upload(self, sql: str, page: int, context: "Context") -> int:
        """Executes the page Query and uploads the results to Cloud Storage

        Args:
            sql (str): The paged SQL query
            page (int): The current page to execute and upload
            context (Context):  Jinja2 template context for task rendering.

        Returns:
            int: The number of uploaded rows in the current page
        """
        data = self._query(sql, context)
        if isinstance(data, pl.DataFrame):
            returned_rows = len(data)
        elif isinstance(data, pd.DataFrame):
            returned_rows = len(data)
        else:
            returned_rows = data.num_rows
        self.log.info(f"Rows fetched for page {page}: {returned_rows}")

        if returned_rows > 0:
            with self._write_local_data_files(data) as file_to_upload:
                file_to_upload.flush()
                self._upload_to_gcs(
                    file_to_upload, f"{self.bucket_dir}/{self.table}_{page}.parquet"
                )
        return returned_rows
