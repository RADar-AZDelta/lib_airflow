import json
from datetime import datetime
from typing import Sequence, cast

import backoff
import polars as pl
from airflow.utils.context import Context

from ....model.bookkeeper import BookkeeperFullUploadTable, BookkeeperTable
from ....model.dbmetadata import Table
from ....utils import AirflowJsonDecoder, AirflowJsonEncoder
from ..full_to_bq_base import FullUploadToBigQueryBaseOperator


class SqlServerFullUploadToBigQueryOperator(FullUploadToBigQueryBaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_get_bookkeeper_table",
        "sql_upsert_bookkeeper_table",
        "sql_paged_full_upload",
        "sql_paged_full_upload_with_cte",
        "sql_topped_full_upload",
        "sql_get_tables_metadata",
    )

    def __init__(
        self,
        sql_paged_full_upload: str = """{% raw %}
SELECT *
FROM [{{ schema }}].[{{ table }}] WITH (NOLOCK)
ORDER BY 1
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        sql_paged_full_upload_with_cte: str = """{% raw %}
WITH cte AS (
    SELECT {{ pk_columns }}
    FROM [{{ schema }}].[{{ table }}] WITH (NOLOCK)
    ORDER BY {{ order_by }}
    OFFSET {{ page * page_size }} ROWS
    FETCH NEXT {{ page_size }} ROWS ONLY
)
SELECT t.*
FROM cte
INNER JOIN [{{ schema }}].[{{ table }}] t WITH (NOLOCK) ON {{ join_clause }}
{% endraw %}""",
        sql_topped_full_upload: str = """{% raw %}
SELECT TOP {{ page_size }} *
FROM [{{ schema }}].[{{ table }}] WITH (NOLOCK)
{%- if where_clause %}
{{ where_clause }}
{% endif %}
ORDER BY {{ order_by }}
{% endraw %}""",
        sql_get_tables_metadata: str = """{% raw %}
SELECT DB_NAME(DB_ID()) AS [database]
    ,s.name AS [schema]
	,t.name AS [table]
	,col.name AS col_name
	,type_name(col.system_type_id) AS system_type
	,type_name(col.user_type_id) AS user_type
	,IIF(ic.index_column_id is null, CAST(0 AS bit), CAST(1 AS bit)) AS is_pk
	,ic.index_column_id
FROM sys.tables t
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
INNER JOIN sys.columns col ON col.object_id = t.object_id
LEFT OUTER JOIN sys.indexes pk ON t.object_id = pk.object_id AND pk.is_primary_key = 1 
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id AND col.column_id = ic.column_id
{%- if where_clause %}
{{ where_clause }}
{% endif %}
ORDER BY [schema], [table], is_pk DESC, ic.index_column_id ASC
{% endraw %}""",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql_paged_full_upload = sql_paged_full_upload
        self.sql_paged_full_upload_with_cte = sql_paged_full_upload_with_cte
        self.sql_topped_full_upload = sql_topped_full_upload
        self.sql_get_tables_metadata = sql_get_tables_metadata

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_tables_chunk(self, table_names: list[str] | None) -> list[Table]:
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_get_tables_metadata)

        where_clause = None
        if table_names:
            where_clause = (
                "WHERE concat(s.name, '.', t.name) IN ('"
                + "','".join(table_names)
                + "')"
            )
        sql = template.render(where_clause=where_clause)

        df = self._query(sql=sql)

        tables = cast(
            list[Table],
            df.sort(by=["database", "schema", "table", "index_column_id"])
            .group_by(
                ["database", "schema", "table"],
            )
            .agg(
                [
                    pl.col("col_name").filter(pl.col("is_pk") == True).alias("pks"),
                    pl.col("system_type")
                    .filter(pl.col("is_pk") == True)
                    .alias("pks_type"),
                    pl.col("col_name")
                    .filter(pl.col("is_pk") == False)
                    .alias("columns"),
                    pl.col("system_type")
                    .filter(pl.col("is_pk") == False)
                    .alias("columns_type"),
                ]
            )
            .sort(by=["database", "schema", "table"])
            .to_dicts(),
        )
        tables_chunk = [
            tables[i :: self.nbr_of_chunks] for i in range(self.nbr_of_chunks)
        ]

        return tables_chunk[self.chunk]

    def _execute_table_ipml(
        self,
        upload_strategy: str,
        table: Table,
        bookkeeper_table: BookkeeperTable,
        context: Context,
    ):
        bookkeeper_table = cast(BookkeeperFullUploadTable, bookkeeper_table)
        if upload_strategy == "paged_full_upload":
            self._paged_full_upload(context, table, bookkeeper_table)
        elif upload_strategy == "paged_full_upload_with_cte":
            self._paged_full_upload_with_cte(context, table, bookkeeper_table)
        elif upload_strategy == "topped_full_upload":
            self._topped_full_upload(context, table, bookkeeper_table)
        else:
            raise Exception(f"Unknown upload strategy: {upload_strategy}")

    def _choose_upload_strategy(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
    ) -> str:
        if not len(table["pks"]):
            upload_strategy = "paged_full_upload"
        elif all(
            item
            in [  # SELECT * FROM sys.types order by 1
                "int",
                "bigint",
                "smallint",
                "tinyint",
                "decimal",
                "float",
                "numeric",
                "char",
                "nchar",
                "nvarchar",
                "varchar",
                # "date",
                "datetime",
                # "datetime2",
                # "smalldatetime",
                # "time",
                "uniqueidentifier",
                "binary",
            ]
            for item in table["pks_type"]
        ):
            upload_strategy = "topped_full_upload"
        else:
            upload_strategy = "paged_full_upload_with_cte"

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def _paged_full_upload(
        self,
        context: Context,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
    ):
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_paged_full_upload)

        page_size = bookkeeper_table["page_size"] or self.page_size
        if not bookkeeper_table["current_page"]:
            bookkeeper_table["current_page"] = 0
        returned_rows = page_size
        while returned_rows == page_size:
            sql = template.render(
                schema=table["schema"],
                table=table["table"],
                page_size=self.page_size,
                page=bookkeeper_table["current_page"],
            )
            df = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table']}/full/{table['table']}_{bookkeeper_table['current_page']}.parquet",
                table_metadata=table,
            )
            returned_rows = len(df)
            bookkeeper_table["current_page"] += 1
            if returned_rows == self.page_size:
                self._full_upload_page_uploaded(table, bookkeeper_table)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table']}/full/{table['table']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table']}/full/{table['table']}_*.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table=table["table"],
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_done(table, bookkeeper_table, context)

    def _paged_full_upload_with_cte(
        self,
        context: Context,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
    ):
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_paged_full_upload_with_cte)

        pk_columns = ", ".join(table["pks"])
        order_by = ", ".join(table["pks"])
        join_clause = " and ".join(
            map(
                lambda column: f"cte.[{column}] = t.[{column}]",
                table["pks"],
            )
        )

        page_size = bookkeeper_table["page_size"] or self.page_size
        if not bookkeeper_table["current_page"]:
            bookkeeper_table["current_page"] = 0
        returned_rows = page_size
        while returned_rows == page_size:
            sql = template.render(
                schema=table["schema"],
                table=table["table"],
                pk_columns=pk_columns,
                order_by=order_by,
                join_clause=join_clause,
                page_size=self.page_size,
                page=bookkeeper_table["current_page"],
            )
            df = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table']}/full/{table['table']}_{bookkeeper_table['current_page']}.parquet",
                table_metadata=table,
            )
            returned_rows = len(df)
            bookkeeper_table["current_page"] += 1
            if returned_rows == self.page_size:
                self._full_upload_page_uploaded(table, bookkeeper_table)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table']}/full/{table['table']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table']}/full/{table['table']}_*.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table=table["table"],
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_done(table, bookkeeper_table, context)

    def _topped_full_upload(
        self,
        context: Context,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
    ):
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_topped_full_upload)

        order_by = ", ".join(table["pks"])

        current_pk = (
            json.loads(bookkeeper_table["current_pk"], cls=AirflowJsonDecoder)
            if bookkeeper_table["current_page"] and bookkeeper_table["current_pk"]
            else None
        )

        page_size = bookkeeper_table["page_size"] or self.page_size
        if not bookkeeper_table["current_page"]:
            bookkeeper_table["current_page"] = 0
        returned_rows = page_size
        while returned_rows == page_size:
            if current_pk:
                where_clause = "WHERE "
                pks = table["pks"].copy()
                while len(pks):
                    if len(pks) < len(table["pks"]):
                        where_clause += " OR "
                    where_clause += "("
                    for index, pk in enumerate(pks):
                        where_clause += " AND " if index > 0 else ""
                        where_clause += pk
                        where_clause += " = " if index < (len(pks) - 1) else " > "
                        match table["pks_type"][index]:
                            case (
                                "int"
                                | "bigint"
                                | "smallint"
                                | "tinyint"
                                | "float"
                                | "decimal"
                                | "numeric"
                            ):
                                where_clause += f"{list(current_pk.values())[index]}"
                            case (
                                "char"
                                | "nchar"
                                | "nvarchar"
                                | "varchar"
                                | "uniqueidentifier"
                            ):
                                where_clause += f"'{list(current_pk.values())[index]}'"
                            case "datetime":
                                d: datetime = list(current_pk.values())[index]
                                iso_format = f"{d.year:04}-{d.month:02}-{d.day:02}T{d.hour:02}:{d.minute:02}:{d.second:02}.{str(d.microsecond).zfill(3):.3s}"
                                where_clause += f"CONVERT(DATETIME, '{iso_format}', 126)"  # yyyy-mm-dd T hh:mm:ss:nnn
                            case "binary":
                                where_clause += (
                                    (
                                        "0x"
                                        + "".join(
                                            [
                                                "{:02x}".format(x)
                                                for x in list(current_pk.values())[
                                                    index
                                                ]
                                            ]
                                        )
                                    )
                                    if list(current_pk.values())[index]
                                    else "0x0"
                                )
                            case _:  # "date" | "datetime2" | "smalldatetime", | "time",
                                breakpoint()
                    where_clause += ")"
                    del pks[-1]

            else:
                where_clause = None

            sql = template.render(
                schema=table["schema"],
                table=table["table"],
                order_by=order_by,
                where_clause=where_clause,
                page_size=self.page_size,
                page=bookkeeper_table["current_page"],
            )
            df = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table']}/full/{table['table']}_{bookkeeper_table['current_page']}.parquet",
                table_metadata=table,
            )
            returned_rows = len(df)
            current_pk = {}
            if not df.is_empty():
                last_row = df.row(-1)
                for pk in table["pks"]:
                    current_pk[pk] = last_row[df.columns.index(pk)]
            bookkeeper_table["current_pk"] = json.dumps(
                current_pk, cls=AirflowJsonEncoder
            )
            bookkeeper_table["current_page"] += 1
            if returned_rows == self.page_size:
                self._full_upload_page_uploaded(table, bookkeeper_table)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table']}/full/{table['table']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table']}/full/{table['table']}_*.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table=table["table"],
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_done(table, bookkeeper_table, context)
