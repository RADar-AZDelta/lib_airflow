import json
import traceback
from typing import Callable, Iterable, List, Optional, Sequence, Tuple, Union, cast

import backoff
import google.cloud.bigquery as bq
import polars as pl
from airflow.utils.context import Context
from airflow.utils.email import send_email

from libs.lib_azdelta.lib_azdelta.json import DateTimeToIsoFormatEncoder

from ....model.bookkeeper import BookkeeperFullUploadTable, BookkeeperTable
from ....model.dbmetadata import Table
from ..query_and_upload_to_bq import QueryAndUploadToBigQueryOperator


class SqlServerFullUploadToBigQueryOperator(QueryAndUploadToBigQueryOperator):
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
        bookkeeper_dataset: str,
        bookkeeper_table: str,
        destination_dataset: str,
        func_get_table_names: Callable[[str], list[str]] | None = None,
        chunk: int = 1,
        nbr_of_chunks: int = 1,
        sql_get_bookkeeper_table: str = """{% raw %}
select database, schema, table, disabled, page_size, current_pk, current_page
from {{ bookkeeper_dataset }}.{{ bookkeeper_table }}
where database = '{{ database }}' and schema = '{{ schema }}' and table = '{{ table }}'
{% endraw %}""",
        sql_upsert_bookkeeper_table: str = """{% raw %}
MERGE {{ bookkeeper_dataset }}.{{ bookkeeper_table }} AS target
USING (SELECT @database as database, @schema as schema, @table as table, @current_pk as current_pk, @current_page as current_page) AS source
ON (target.database = source.database and target.schema = source.schema and target.table = source.table)
    WHEN MATCHED THEN
        UPDATE SET current_pk = source.current_pk,
            current_page = source.current_page
    WHEN NOT MATCHED THEN
        INSERT (database, schema, table, current_pk, current_page)
        VALUES (source.database, source.schema, source.table, source.current_pk, source.current_page);
{% endraw %}""",
        sql_paged_full_upload: str = """{% raw %}
SELECT *
FROM {{ schema }}.[{{ table }}] WITH (NOLOCK)
ORDER BY 1
OFFSET {{ page * page_size }} ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        sql_paged_full_upload_with_cte: str = """{% raw %}
WITH cte AS (
    SELECT {{ pk_columns }}
    FROM {{ schema }}.[{{ table }}] WITH (NOLOCK)
    ORDER BY {{ order_by }}
    OFFSET {{ page * page_size }} ROWS
    FETCH NEXT {{ page_size }} ROWS ONLY
)
SELECT t.*
FROM cte
INNER JOIN {{ schema }}.[{{ table }}] t WITH (NOLOCK) ON {{ join_clause }}
{% endraw %}""",
        sql_topped_full_upload: str = """{% raw %}
SELECT TOP {{ page_size }} *
FROM {{ schema }}.[{{ table }}] WITH (NOLOCK)
{%- if where_clause %}
{{ where_clause }}
{% endif -%}
ORDER BY {{ order_by }}
{% endraw %}""",
        sql_get_tables_metadata: str = """{% raw %}
SELECT DB_NAME(DB_ID()) as [database]
    ,s.name as [schema]
	,t.name AS [table]
	,col.name as col_name
	,type_name(col.system_type_id) AS system_type
	,type_name(col.user_type_id) AS user_type
	,IIF(ic.index_column_id is null, cast(0 as bit), cast(1 as bit)) as is_pk
	,col.is_identity
	,ic.index_column_id
FROM sys.tables t
INNER JOIN sys.schemas s on s.schema_id = t.schema_id
inner join sys.columns col on col.object_id = t.object_id
left outer join sys.indexes pk on t.object_id = pk.object_id and pk.is_primary_key = 1 
left outer join sys.index_columns ic on ic.object_id = pk.object_id and ic.index_id = pk.index_id and col.column_id = ic.column_id
{%- if where_clause %}
{{ where_clause }}
{% endif %}
order by [schema], [table], is_pk desc, ic.index_column_id asc
{% endraw %}""",
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        to_email_on_error: list[str] | Iterable[str] | None = None,
        func_before_execute_table: Callable[[Table, Context], bool] | None = None,
        func_after_execute_table: Callable[[Table, Context], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.chunk = chunk
        self.nbr_of_chunks = nbr_of_chunks
        self.func_get_table_names = func_get_table_names
        self.bookkeeper_dataset = bookkeeper_dataset
        self.bookkeeper_table = bookkeeper_table
        self.sql_get_bookkeeper_table = sql_get_bookkeeper_table
        self.sql_upsert_bookkeeper_table = sql_upsert_bookkeeper_table
        self.sql_paged_full_upload = sql_paged_full_upload
        self.sql_paged_full_upload_with_cte = sql_paged_full_upload_with_cte
        self.sql_topped_full_upload = sql_topped_full_upload
        self.sql_get_tables_metadata = sql_get_tables_metadata
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.destination_dataset = destination_dataset
        self.to_email_on_error = to_email_on_error
        self.func_before_execute_table = func_before_execute_table
        self.func_after_execute_table = func_after_execute_table

        self._db_hook = None

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
            where_clause = f"WHERE t.name IN ('" + "','".join(table_names) + "')"
        sql = template.render(where_clause=where_clause)

        df = self._query(sql=sql)

        tables = cast(
            list[Table],
            df.sort(by=["database", "schema", "table", "index_column_id"])
            .groupby(
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
                    pl.col("is_identity")
                    .filter((pl.col("is_pk") == True) & (pl.col("is_identity") == True))
                    .first()
                    .alias("is_identity"),
                ]
            )
            .sort(by=["database", "schema", "table"])
            .to_dicts(),
        )
        tables_chunk = [
            tables[i :: self.nbr_of_chunks] for i in range(self.nbr_of_chunks)
        ]

        return tables_chunk[self.chunk]

    def execute(self, context):
        self._before_execute(context)
        table_names = (
            self.func_get_table_names(self.connectorx_source_db_conn_id)
            if self.func_get_table_names
            else None
        )
        tables: list[Table] = self._get_tables_chunk(table_names)

        for table in tables:
            str_error = None
            try:
                bookkeeper_table = self._get_bookkeeper_table(table)
                if bookkeeper_table and bookkeeper_table["disabled"]:
                    self.log.info(f"Table %s is disabled!", table["table"])
                    continue
                if self.func_before_execute_table:
                    continue_table_execute = self.func_before_execute_table(
                        table, context
                    )
                    if not continue_table_execute:
                        self.log.info(f"Skipping table %s", table["table"])
                        continue
                self.execute_table(
                    table,
                    cast(BookkeeperFullUploadTable, bookkeeper_table),
                    context,
                )
                if self.func_after_execute_table:
                    self.func_after_execute_table(table, context)
            except Exception as e:
                str_error = traceback.format_exc()
                self.log.error(f"ERROR SYNCING TABLE {table['table']}\r\n{str_error}")

            if str_error:
                try:
                    if self.to_email_on_error:
                        send_email(
                            to=self.to_email_on_error,
                            subject=f"AIRFLOW ERROR in dag '{self.dag_id}' for table '{table['table']}'",
                            html_content=str_error.replace("\n", "<br />"),
                        )
                except:
                    pass

    def _before_execute(self, context):
        return None

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=5,
    )
    def execute_table(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
        context: Context,
    ):
        self.log.info("Table: %s", table["table"])
        bookkeeper_table = cast(BookkeeperFullUploadTable, bookkeeper_table)

        if not bookkeeper_table["current_page"]:
            self._cleanup_previous_upload(table)

        upload_strategy = self._choose_upload_strategy(table, bookkeeper_table)
        if upload_strategy == "paged_full_upload":
            self._paged_full_upload(context, table, bookkeeper_table)
        elif upload_strategy == "paged_full_upload_with_cte":
            self._paged_full_upload_with_cte(context, table, bookkeeper_table)
        elif upload_strategy == "topped_full_upload":
            self._topped_full_upload(context, table, bookkeeper_table)
        else:
            raise Exception(f"Unknown upload strategy: {upload_strategy}")

    def _cleanup_previous_upload(self, table: Table):
        hook = self._get_gcs_hook()
        objects_length = 1
        while objects_length > 0:
            objects = hook.list(
                self.bucket,
                max_results=100,
                prefix=f"{self.bucket_dir}/{table['table']}/full/",
            )

            objects_length = len(objects)
            for object_name in objects:
                hook.delete("azdelta-landing-zone", object_name)

    def _choose_upload_strategy(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
    ):
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
            ]
            for item in table["pks_type"]
        ):
            upload_strategy = "topped_full_upload"
        else:
            upload_strategy = "paged_full_upload_with_cte"

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def _empty_bookkeeper_table_record(self, table: Table) -> BookkeeperTable:
        return cast(
            BookkeeperFullUploadTable,
            {
                "database": table["database"],
                "schema": table["schema"],
                "table": table["table"],
                "disabled": None,
                "page_size": None,
                "bulk_upload_page": None,
                "current_pk": None,
                "current_page": None,
            },
        )

    def _get_bookkeeper_table(self, table: Table) -> BookkeeperTable:
        jinja_env = self.get_template_env()

        hook = self._get_bq_hook()
        template = jinja_env.from_string(self.sql_get_bookkeeper_table)
        sql = template.render(
            bookkeeper_dataset=self.bookkeeper_dataset,
            bookkeeper_table=self.bookkeeper_table,
            database=table["database"],
            schema=table["schema"],
            table=table["table"],
        )
        client = hook.get_client()
        job_config = bq.QueryJobConfig(
            query_parameters=[],
        )
        query_job = client.query(sql, job_config=job_config, location=self.gcp_location)
        rows = query_job.result()
        records = [dict(row.items()) for row in rows]
        bookkeeper_table = cast(BookkeeperTable, records[0]) if records else None

        if not bookkeeper_table:
            bookkeeper_table = self._empty_bookkeeper_table_record(table)
        return bookkeeper_table

    def _upsert_bookkeeper_table(
        self,
        bookkeeper_table: BookkeeperTable,
    ):
        bookkeeper_table = cast(BookkeeperFullUploadTable, bookkeeper_table)
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_upsert_bookkeeper_table)
        query = template.render(
            bookkeeper_dataset=self.bookkeeper_dataset,
            bookkeeper_table=self.bookkeeper_table,
        )
        hook = self._get_bq_hook()
        client = hook.get_client()
        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter(
                    "database", "STRING", bookkeeper_table["database"]
                ),
                bq.ScalarQueryParameter("schema", "STRING", bookkeeper_table["schema"]),
                bq.ScalarQueryParameter("table", "STRING", bookkeeper_table["table"]),
                bq.ScalarQueryParameter(
                    "current_page",
                    "INTEGER",
                    bookkeeper_table["current_page"],
                ),
                bq.ScalarQueryParameter(
                    "current_pk",
                    "STRING",
                    bookkeeper_table["current_pk"],
                ),
            ],
        )
        query_job = client.query(
            query, job_config=job_config, location=self.gcp_location
        )
        query_job.result()

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
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table']}",
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
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table']}",
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
            json.loads(bookkeeper_table["current_pk"])
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
                            case "int" | "bigint" | "smallint" | "tinyint" | "float" | "decimal" | "numeric":
                                where_clause += f"{list(current_pk.values())[index]}"
                            case "char" | "nchar" | "nvarchar" | "varchar" | "uniqueidentifier":
                                where_clause += f"'{list(current_pk.values())[index]}'"
                            case "datetime":
                                where_clause += f"CONVERT(DATETIME, '{list(current_pk.values())[index].isoformat()}', 126)"
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
                current_pk, cls=DateTimeToIsoFormatEncoder
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
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table']}",
                cluster_fields=self._get_cluster_fields(table),
            )

        self._full_upload_done(table, bookkeeper_table, context)

    def _full_upload_page_uploaded(
        self,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
    ) -> None:
        self._upsert_bookkeeper_table(bookkeeper_table)

    def _full_upload_done(
        self,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
        context: Context,
    ) -> None:
        bookkeeper_table["current_pk"] = None
        bookkeeper_table["current_page"] = None
        self._upsert_bookkeeper_table(bookkeeper_table)

    def _get_cluster_fields(self, table: Table) -> List[str]:
        cluster_fields = [
            column
            for i, column in enumerate(table["pks"] or [])
            if table["pks_type"][i] != "float"
            and table["pks_type"][i]
            != "numeric"  # cluster fields cannot be float or numeric
        ][:4]
        return cluster_fields
