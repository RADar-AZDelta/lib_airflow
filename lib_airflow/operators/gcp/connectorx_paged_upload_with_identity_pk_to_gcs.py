# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""ConnectorX to GCS operator."""

from typing import Any, Callable, Optional, Sequence, Union, cast

import polars as pl
from airflow.utils.context import Context

from .connectorx_to_gcs import ConnectorXToGCSOperator


class ConnectorXPagedUploadWithIdentityPkToGCSOperator(ConnectorXToGCSOperator):
    """Do a paged upload of a table that has a primary key that is auto incremented (identity property)."""

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
        """Constructor

        Args:
            table (str): Name of the table
            identity_column (str): Name of the auto-incremented primary key
            sql (str, optional): The paged query.
            max_identity_sql (str, optional): Query that gets the largest value of the primary key, currently in the database.
            start_identity (Union[int, str], optional): Identity to start from. Defaults to 0.
            page_size (Union[int, str], optional): Number of rows to fetch in each page. Defaults to 100000.
            bucket_dir (str, optional): The bucket dir to store the paged upload Parquet files. Defaults to "upload".
            func_page_loaded (Optional[Callable[[str, Context, int], None]], optional): Function that can be called every time a page is uploaded. Defaults to None.
        """
        super().__init__(sql=sql, **kwargs)

        self.table = table
        self.identity_column = identity_column
        self.max_identity_sql = max_identity_sql
        self.start_identity = start_identity
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.func_page_loaded = func_page_loaded

    def execute(self, context: "Context") -> None:
        """Execute the operator.
        Dd the paged SQL query and upload the results as Parquet to Cloud Storage.

        Args:
            context (Context):  Jinja2 template context for task rendering.
        """
        jinja_env = self.get_template_env()

        max_identity_value = self._get_max_identity_value() or 0
        current_identity_value_lower = cast(int, self.start_identity) or 0
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
                sql, current_identity_value_lower // cast(int, self.page_size), context
            )
            current_identity_value_lower += cast(int, self.page_size)
            if self.func_page_loaded:
                self.func_page_loaded(
                    self.table,
                    context,
                    current_identity_value_lower,
                )

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

    def _get_max_identity_value(self) -> Any:
        """Executes the query that gets the largest value of the primary key, currently in the database..

        Returns:
            Any: The largest value, currently in our database, of our auto incremented primary key
        """
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.max_identity_sql)
        sql = template.render(
            table=self.table,
            identity_column=self.identity_column,
        )
        hook = self._get_connectorx_hook()
        table = hook.get_arrow_table(sql)
        return table["max_identity_value"][0].as_py()
