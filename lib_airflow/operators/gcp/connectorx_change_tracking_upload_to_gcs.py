# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

"""ConnectorX to GCS operator."""

import functools as ft
from typing import Any, Callable, List, Optional, Sequence, Tuple, Union

import polars as pl
import pyarrow as pa
from airflow.utils.context import Context

from .connectorx_to_gcs import ConnectorXToGCSOperator


class ConnectorXChangeTrackingUploadToGCSOperator(ConnectorXToGCSOperator):
    """Does an incremental upload to Cloud Stoarage of a MSSQL table that has [Change Tracking](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server?view=sql-server-ver16) enabled"""

    template_fields: Sequence[str] = (
        "bucket",
        "impersonation_chain",
        "bucket_dir",
        "last_synchronization_version",
        "page_size",
        "table",
        "change_tracking_table",
        "pk_columns",
        "columns",
        "sql",
        "pk_columns_sql",
    )
    ui_color = "#e0a98c"

    def __init__(
        self,
        *,
        table: str,
        change_tracking_table: Optional[str] = None,
        pk_columns: Union[List[str], str],
        columns: Union[List[str], str],
        last_synchronization_version: Union[int, str],
        sql: str = """{% raw %}
SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, {{ pk_columns }}, {{ columns }}
FROM CHANGETABLE(CHANGES {{ table }}, {{ last_synchronization_version }}) AS ct
left outer join {{ table }} t on {{ join_on_clause }}
ORDER BY SYS_CHANGE_VERSION
OFFSET 0 ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        page_size: Union[int, str] = 100000,
        pk_columns_sql: str = """{% raw %}
select col.name as pk_col_name
from sys.tables t
inner join sys.indexes pk on t.object_id = pk.object_id
inner join sys.index_columns ic on ic.object_id = pk.object_id and ic.index_id = pk.index_id
inner join sys.columns col on pk.object_id = col.object_id and col.column_id = ic.column_id
where pk.is_primary_key = 1
	and t.name = '{{ change_tracking_table }}'
{% endraw %}""",
        bucket_dir: str = "upload",
        func_till_sys_change_version_loaded: Optional[
            Callable[[str, Context, int], None]
        ] = None,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            table (str): The name of the database table
            pk_columns (Union[List[str], str], optional): List of the primary key columns. Defaults to None.
            columns (Union[List[str], str]): List of table columns.
            last_synchronization_version (Union[int, str]): The last synchronised version
            change_tracking_table (Optional[str], optional): Name of the change tracking table. Defaults to None.
            sql (str, optional): Query to retrieve all changes on the table since the last synchronised version.
            page_size (Union[int, str], optional): Number of rows to fetch in each page. Defaults to 100000.
            pk_columns_sql (str, optional): Query to retrieve the list of primary key columns.
            bucket_dir (str, optional): The bucket dir to store the paged upload Parquet files. Defaults to "upload".
            func_till_sys_change_version_loaded (Optional[ Callable[[str, Context, int], None] ], optional):
            Function that can be used to store the last synchronised version, so that it can be used in the next run. Defaults to None.
        """
        super().__init__(sql=sql, **kwargs)

        self.table = table
        self.change_tracking_table = change_tracking_table
        self.pk_columns = pk_columns
        self.columns = columns
        self.last_synchronization_version = last_synchronization_version
        self.page_size = page_size
        self.pk_columns_sql = pk_columns_sql
        self.bucket_dir = bucket_dir
        self.func_till_sys_change_version_loaded = func_till_sys_change_version_loaded

    def execute(self, context: "Context") -> None:
        """Executes the operator.
        Executes the query the get all the changes of a table since the specified last synchronised version.
        Uploads the results as Parquet to Cloud Storage.

        Args:
            context (Context):  Jinja2 template context for task rendering.
        """
        jinja_env = self.get_template_env()

        change_tracking_pk_columns = None
        if self.change_tracking_table:
            change_tracking_pk_columns = self._get_change_tracking_table_pks()
        else:
            self.change_tracking_table = self.table
        if not change_tracking_pk_columns:
            change_tracking_pk_columns = self.pk_columns

        pks: List[Tuple[str, str]] = [
            (pk_column, change_tracking_pk_columns[idx])
            for idx, pk_column in enumerate(self.pk_columns)
        ]
        join_on_clause = ft.reduce(
            lambda a, b: a + " and " + b,
            map(lambda pk: f"t.[{pk[0]}] = ct.[{pk[1]}]", pks),
        )

        template = jinja_env.from_string(self.sql)
        sql = template.render(
            pk_columns=", ".join(map(lambda pk: f"ct.[{pk[1]}]", pks)),
            columns=", ".join(map(lambda column: f"t.[{column}]", self.columns)),
            table=self.table,
            last_synchronization_version=self.last_synchronization_version,
            join_on_clause=join_on_clause,
            page_size=self.page_size,
        )

        df = self._query(sql, context)
        if isinstance(df, pa.Table):
            df = pl.from_arrow(df)
        returned_rows = len(df)
        self.log.info(f"Rows fetched: {returned_rows}")

        if returned_rows == 0:
            return

        last_synchronization_version = df["SYS_CHANGE_VERSION"].max()
        df = df.unique(subset=change_tracking_pk_columns, keep="last")
        for pk in (
            pk for pk in pks if pk[0] != pk[1]
        ):  # replace/remove the change_tracking_table pk's if table not equals change_tracking_table
            df.replace(pk[0], df[pk[1]])
            df.drop_in_place(pk[1])
        df = df.with_column(
            pl.when(pl.col("SYS_CHANGE_OPERATION") == "D")
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("deleted")
        )  # add deleted column
        df.drop_in_place("SYS_CHANGE_OPERATION")
        df.drop_in_place("SYS_CHANGE_VERSION")

        with self._write_local_data_files(df) as file_to_upload:
            file_to_upload.flush()
            self._upload_to_gcs(
                file_to_upload,
                f"{self.bucket_dir}/{self.table}_{self.last_synchronization_version}.parquet",
            )

        if self.func_till_sys_change_version_loaded:
            self.func_till_sys_change_version_loaded(
                self.table, context, last_synchronization_version
            )

        context["task_instance"].xcom_push(
            key="last_synchronization_version", value=last_synchronization_version
        )

    def _get_change_tracking_table_pks(self) -> Any:
        """Gets the primary keys from the change tracking table using the provided pk_columns_sql SQL.

        Returns:
            Any: The change tracking table primary keys.
        """
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.pk_columns_sql)
        sql = template.render(
            change_tracking_table=self.change_tracking_table,
        )
        hook = self._get_connectorx_hook()
        table = hook.get_arrow_table(sql)
        return table[0].as_py()
