import traceback
from typing import Callable, Iterable, List, Sequence, Tuple, Union, cast

from airflow.utils.context import Context
from airflow.utils.email import send_email

from ...model.dbmetadata.table import Table
from .query_and_upload_to_bq import QueryAndUploadToBigQueryOperator


class FullUploadToBigQueryOperator(QueryAndUploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_full_upload",
        "sql_full_upload_with_identity_pk",
        "max_identity_sql",
        "sql_full_upload_with_single_nvarchar_pk",
    )

    def __init__(
        self,
        # tables: List[ChangeTrackingTable],
        chunk: int,
        func_get_tables: Callable[[str, int], List[Table]],
        destination_dataset: str,
        sql_full_upload: str = """{% raw %}
{% if pk_columns %}
with cte as (
	select {{ pk_columns }}
	from {{ schema }}.[{{ table }}] with (nolock)
	ORDER BY {{ order_by }}
    OFFSET {{ page * page_size }} ROWS
    FETCH NEXT {{ page_size }} ROWS ONLY
)
select t.*
from cte 
inner join {{ schema }}.[{{ table }}] t with (nolock) on {{ join_clause }}
{% else %}
select *
from {{ schema }}.[{{ table }}] with (nolock)
ORDER BY {{ order_by }}
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endif %}
{% endraw %}""",
        sql_full_upload_with_identity_pk: str = """{% raw %}
select top {{ page_size }} *
from {{ schema }}.[{{ table }}] with (nolock)
where {{ identity_column }} > {{ current_identity_value_lower }}
order by {{ identity_column }} asc
{% endraw %}""",
        max_identity_sql: str = """{% raw %}
select max({{ identity_column }}) as max_identity_value
from {{ schema }}.[{{ table }}]
{% endraw %}""",
        sql_full_upload_with_single_nvarchar_pk: str = """{% raw %}
with cte as (
	select top {{ page_size }} {{ pk_column }}
	from {{ schema }}.[{{ table }}] with (nolock)
    where {{ pk_column }} > '{{ current_pk_value }}'
	ORDER BY {{ pk_column }}
)
select t.*
from cte 
inner join {{ schema }}.[{{ table }}] t with (nolock) on t.{{ pk_column }} = cte.{{ pk_column }}
{% endraw %}""",
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        to_email_on_error: list[str] | Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.chunk = chunk
        self.func_get_tables = func_get_tables
        self.sql_full_upload = sql_full_upload
        self.sql_full_upload_with_identity_pk = sql_full_upload_with_identity_pk
        self.max_identity_sql = max_identity_sql
        self.sql_full_upload_with_single_nvarchar_pk = (
            sql_full_upload_with_single_nvarchar_pk
        )
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.destination_dataset = destination_dataset
        self.to_email_on_error = to_email_on_error

        self._db_hook = None

    def execute(self, context):
        self._before_execute(context)
        tables = self.func_get_tables(self.connectorx_source_db_conn_id, self.chunk)
        for table in tables:
            str_error = None
            try:
                self.execute_table(table, context)
            except Exception as e:
                str_error = traceback.format_exc()
                self.log.error(
                    f"ERROR SYNCING TABLE {table['table_name']}\r\n{str_error}"
                )

            if str_error:
                try:
                    if self.to_email_on_error:
                        send_email(
                            to=self.to_email_on_error,
                            subject=f"AIRFLOW ERROR in dag '{self.dag_id}' for table '{table['table_name']}'",
                            html_content=str_error.replace("\n", "<br />"),
                        )
                except:
                    pass

    def _before_execute(self, context):
        return None

    def execute_table(
        self,
        table: Table,
        context: Context,
    ):
        upload_strategy = self._choose_upload_strategy(table)
        if upload_strategy == "full_upload":
            self._full_upload(context, table)
        elif upload_strategy == "full_upload_with_identity_pk":
            self._full_upload_with_identity_pk(context, table)
        elif upload_strategy == "full_upload_with_single_nvarchar_pk":
            self._full_upload_with_single_nvarchar_pk(context, table)
        else:
            raise Exception(f"Unknown upload strategy: {upload_strategy}")

    def _choose_upload_strategy(
        self,
        table: Table,
    ):
        if table["is_identity"]:
            upload_strategy = "full_upload_with_identity_pk"
        elif len(table["pks"]) == 1 and table["pks_type"][0] == "nvarchar":
            upload_strategy = "full_upload_with_single_nvarchar_pk"
        else:
            upload_strategy = "full_upload"

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def get_page_size(self, table: Table):
        return self.page_size

    def _full_upload(
        self,
        context: Context,
        table: Table,
    ):
        jinja_env = self.get_template_env()
        pk_columns = ", ".join(table["pks"]) if table["pks"] else None
        order_by = ", ".join(table["pks"]) if table["pks"] else "1"

        join_clause = (
            " and ".join(
                map(
                    lambda column: f"cte.[{column}] = t.[{column}]",
                    table["pks"],
                )
            )
            if table["pks"]
            else None
        )

        page = self._full_upload_get_start_page(table)
        page_size = self.get_page_size(table)
        returned_rows = page_size
        while returned_rows == page_size:
            template = jinja_env.from_string(self.sql_full_upload)
            sql = template.render(
                schema=table["schema_name"],
                table=table["table_name"],
                pk_columns=pk_columns,
                order_by=order_by,
                join_clause=join_clause,
                page_size=self.page_size,
                page=page,
            )
            returned_rows = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_{page}.parquet",
                table_metadata=table,
            )
            page += 1
            if returned_rows == self.page_size:
                self._full_upload_page_uploaded(table, page)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table_name']}",
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_done(table)

    def _full_upload_with_identity_pk(
        self,
        context: Context,
        table: Table,
    ):
        jinja_env = self.get_template_env()
        (
            current_identity_value_lower,
            page,
        ) = self._full_upload_with_identity_pk_get_start_identity(context, table)
        page_size = self.get_page_size(table)
        max_identity_value = self._get_max_identity_value(table) or 0
        total_returned_rows = 0

        while current_identity_value_lower < max_identity_value:
            template = jinja_env.from_string(self.sql_full_upload_with_identity_pk)
            sql = template.render(
                schema=table["schema_name"],
                table=table["table_name"],
                identity_column=table["pks"][0],
                current_identity_value_lower=current_identity_value_lower,
                page_size=page_size,
            )
            df = self._query(sql)
            returned_rows = self._upload_parquet(
                df,
                object_name=f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_{page}.parquet",
                table_metadata=table,
            )
            total_returned_rows += returned_rows
            current_identity_value_lower = cast(
                int, df.get_column(table["pks"][0]).max()
            )
            page += 1
            self._full_upload_with_identity_pk_page_uploaded(
                table, current_identity_value_lower, page
            )

        self.log.info("Uploaded %s rows", total_returned_rows)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table_name']}",
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_with_identity_pk_done(table)

    def _full_upload_with_single_nvarchar_pk(
        self,
        context: Context,
        table: Table,
    ):
        jinja_env = self.get_template_env()
        current_pk_value, page = self._full_upload_with_single_nvarchar_pk_get_start_pk(
            context, table
        )
        page_size = self.get_page_size(table)
        returned_rows = page_size

        while returned_rows == page_size:
            template = jinja_env.from_string(
                self.sql_full_upload_with_single_nvarchar_pk
            )
            sql = template.render(
                schema=table["schema_name"],
                table=table["table_name"],
                pk_column=table["pks"][0],
                current_pk_value=current_pk_value,
                page_size=page_size,
            )
            df = self._query(sql)
            returned_rows = self._upload_parquet(
                df,
                object_name=f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_{page}.parquet",
                table_metadata=table,
            )
            if not df.is_empty():
                current_pk_value = cast(str, df[table["pks"][0]].sort()[-1])
            page += 1
            if returned_rows == page_size:
                self._full_upload_with_single_nvarchar_pk_page_uploaded(
                    table, current_pk_value, page
                )

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table_name']}",
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_with_single_nvarchar_pk_done(table)

    def _full_upload_get_start_page(self, table: Table) -> int:
        return 0

    def _full_upload_page_uploaded(self, table: Table, page: int) -> None:
        return None

    def _full_upload_done(self, table: Table) -> None:
        return None

    def _full_upload_with_identity_pk_get_start_identity(
        self,
        context: Context,
        table: Table,
    ) -> Tuple[int, int]:
        return (0, 0)

    def _full_upload_with_identity_pk_page_uploaded(
        self, table: Table, identity: int, page: int
    ) -> None:
        return None

    def _full_upload_with_identity_pk_done(self, table: Table) -> None:
        return None

    def _full_upload_with_single_nvarchar_pk_get_start_pk(
        self,
        context: Context,
        table: Table,
    ) -> Tuple[str, int]:
        return ("", 0)

    def _full_upload_with_single_nvarchar_pk_page_uploaded(
        self, table: Table, pk: str, page: int
    ) -> None:
        return None

    def _full_upload_with_single_nvarchar_pk_done(self, table: Table) -> None:
        return None

    def _get_cluster_fields(self, table: Table) -> List[str]:
        cluster_fields = [
            column
            for i, column in enumerate(table["pks"] or [])
            if table["pks_type"][i] != "float"
            and table["pks_type"][i]
            != "numeric"  # cluster fields cannot be float or numeric
        ][:4]
        return cluster_fields

    def _get_max_identity_value(self, table: Table):
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.max_identity_sql)
        sql = template.render(
            schema=table["schema_name"],
            table=table["table_name"],
            identity_column=table["pks"][0],
        )
        df = self._query(sql)
        return cast(int, df["max_identity_value"][0])
