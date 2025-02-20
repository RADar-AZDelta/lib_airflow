from typing import Sequence, cast

import google.cloud.bigquery as bq
import polars as pl
from airflow.utils.context import Context

from ....model.bookkeeper import (
    BookkeeperFullUploadTable,
    BookkeeperHixDxhTable,
    BookkeeperTable,
)
from ....model.dbmetadata import Table
from .full_to_bq import SqlServerFullUploadToBigQueryOperator


class HixDwhToBigQueryOperator(SqlServerFullUploadToBigQueryOperator):
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
        sql_get_bookkeeper_table: str = """{% raw %}
select database, schema, table, disabled, page_size, current_pk, current_page, cycleid
from `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}`
where database = '{{ database }}' and schema = '{{ schema }}' and table = '{{ table }}'
{% endraw %}""",
        sql_upsert_bookkeeper_table: str = """{% raw %}
MERGE `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}` AS target
USING (SELECT @database as database, @schema as schema, @table as table, @current_pk as current_pk, @current_page as current_page, @cycleid as cycleid) AS source
ON (target.database = source.database and target.schema = source.schema and target.table = source.table)
    WHEN MATCHED THEN
        UPDATE SET current_pk = source.current_pk,
            current_page = source.current_page,
            cycleid = source.cycleid
    WHEN NOT MATCHED THEN
        INSERT (database, schema, table, current_pk, current_page, cycleid)
        VALUES (source.database, source.schema, source.table, source.current_pk, source.current_page, source.cycleid);
{% endraw %}""",
        sql_last_cycleid="select max(cycleid) as last_cycle_id from hdw.CycleLog with (nolock) where EndDateUtc <= GETDATE()",
        sql_get_tables_metadata: str = """{% raw %}
SELECT DB_NAME(DB_ID()) as [database]
    ,s.name as [schema]
	,t.name AS [table]
	,col.name as col_name
	,type_name(col.system_type_id) AS system_type
	,type_name(col.user_type_id) AS user_type
	,IIF(ic.index_column_id is null, cast(0 as bit), cast(1 as bit)) as is_pk
	,ic.index_column_id
FROM sys.tables t
INNER JOIN sys.schemas s on s.schema_id = t.schema_id
inner join sys.columns col on col.object_id = t.object_id
left outer join sys.indexes pk on t.object_id = pk.object_id and pk.is_primary_key = 1 
left outer join sys.index_columns ic on ic.object_id = pk.object_id and ic.index_id = pk.index_id and col.column_id = ic.column_id
{%- if where_clause %}
{{ where_clause }}
    and s.name = 'hdw'
{% else %}
where s.name = 'hdw'
{% endif %}
order by [schema], [table], is_pk desc, ic.index_column_id asc
{% endraw %}""",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql_get_bookkeeper_table = sql_get_bookkeeper_table
        self.sql_upsert_bookkeeper_table = sql_upsert_bookkeeper_table
        self.sql_last_cycleid = sql_last_cycleid
        self.sql_get_tables_metadata = sql_get_tables_metadata

        self.last_cycle_id = 0

    def _before_execute(self, context):
        self.last_cycle_id = self._get_last_cycle_id()

    def _empty_bookkeeper_table_record(self, table: Table) -> BookkeeperTable:
        return cast(
            BookkeeperHixDxhTable,
            {
                "database": table["database"],
                "schema": table["schema"],
                "table": table["table"],
                "disabled": None,
                "page_size": None,
                "bulk_upload_page": None,
                "current_pk": None,
                "current_page": None,
                "cycleid": None,
            },
        )

    def _upsert_bookkeeper_table(
        self,
        bookkeeper_table: BookkeeperTable,
    ):
        bookkeeper_table = cast(BookkeeperHixDxhTable, bookkeeper_table)
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
                bq.ScalarQueryParameter(
                    "cycleid",
                    "INTEGER",
                    bookkeeper_table["cycleid"],
                ),
            ],
        )
        query_job = client.query(
            query, job_config=job_config, location=self.gcp_location
        )
        query_job.result()

    def execute_table(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
        context: Context,
    ):
        self.log.info("Table: %s", table["table"])

        upload_strategy = self._choose_upload_strategy(table, bookkeeper_table)

        bookkeeper_table = cast(BookkeeperHixDxhTable, bookkeeper_table)

        if upload_strategy == "cycle_upload":
            if not bookkeeper_table["cycleid"]:
                bookkeeper_table["cycleid"] = 0
            while bookkeeper_table["cycleid"] <= self.last_cycle_id:
                self._upload_cycle(context, table, bookkeeper_table)
        else:
            super().execute_table(table, bookkeeper_table, context)
            # self._cleanup_previous_upload(table)
            # self._full_upload(context, table, bookkeeper_table)
            bookkeeper_table["cycleid"] = self.last_cycle_id
            self._cycle_upload_done(table, bookkeeper_table, context)

    def _choose_upload_strategy(self, table: Table, bookkeeper_table: BookkeeperTable):
        bookkeeper_table = cast(BookkeeperHixDxhTable, bookkeeper_table)
        if bookkeeper_table["cycleid"]:
            upload_strategy = "cycle_upload"
        else:
            upload_strategy = super()._choose_upload_strategy(table, bookkeeper_table)

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def _get_last_cycle_id(self) -> int:
        df = self._query(self.sql_last_cycleid)
        return df["last_cycle_id"][0]

    def _upload_cycle(
        self, context: Context, table: Table, bookkeeper_table: BookkeeperHixDxhTable
    ):
        if (bookkeeper_table["cycleid"] or 0) > self.last_cycle_id:
            self.log.info(
                f"Cycle {self.last_cycle_id} already uploaded for table {table['table']}"
            )
            return

        self.log.info(
            f"Append cycle {bookkeeper_table['cycleid']} to table {table['table']}"
        )
        jinja_env = self.get_template_env()

        pk_is_shakey = any(pk for pk in table["pks"] if pk.endswith("Sha1Key"))
        if pk_is_shakey:
            table_sequel = (
                "Link"
                if table["table"].endswith("LinkSat") or table["table"].endswith("Link")
                else ""
            )
            pk_name = f"Hdw{table_sequel}Sha1Key"
        else:
            pk_name = [pk for pk in table["pks"] if pk != "HdwLoadDateUtc"][0]

        if not bookkeeper_table["current_page"]:
            bookkeeper_table["current_page"] = 0
        page_size = bookkeeper_table["page_size"] or self.page_size
        returned_rows = page_size
        while returned_rows == page_size:
            template = jinja_env.from_string(
                """
with cte as (
	select {{ pk_name }}, HdwLoadDateUtc
	from hdw.{{ table }}
	where HdwCycleId = {{ cycle_id }}
	order by HdwLoadDateUtc
	offset {{ page }} * {{ page_size }} rows
	fetch next {{ page_size }} rows only
)
select t.*
from cte
inner join {{ schema }}.{{ table }} t on t.{{ pk_name }} = cte.{{ pk_name }} and t.HdwLoadDateUtc = cte.HdwLoadDateUtc;
"""
            )
            sql = template.render(
                schema=table["schema"],
                table=table["table"],
                pk_name=pk_name,
                cycle_id=bookkeeper_table["cycleid"],
                page_size=self.page_size,
                page=bookkeeper_table["current_page"],
            )
            df = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table']}/cycle_{bookkeeper_table['cycleid']}/{table['table']}_{bookkeeper_table['cycleid']}_{bookkeeper_table['current_page']}.parquet",
                table_metadata=table,
            )
            returned_rows = len(df)
            bookkeeper_table["current_page"] += 1
            if returned_rows == self.page_size:
                self._cycle_upload_page_uploaded(table, bookkeeper_table, context)

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table']}/cycle_{bookkeeper_table['cycleid']}/{table['table']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table']}/cycle_{bookkeeper_table['cycleid']}/{table['table']}_*.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table=table["table"],
                # cluster_fields=self._get_cluster_fields(table), # "Clustering is not supported on non-orderable column 'HdwLinkSha1Key' of type 'STRUCT<list ARRAY<STRUCT<item INT64>>>'"
                write_disposition="WRITE_APPEND",
                schema_update_options=["ALLOW_FIELD_ADDITION"],
            )

        bookkeeper_table["cycleid"] = (bookkeeper_table["cycleid"] or 0) + 1
        self._cycle_upload_done(table, bookkeeper_table, context)

    def _cycle_upload_done(
        self,
        table: Table,
        bookkeeper_table: BookkeeperHixDxhTable,
        context: Context,
    ) -> None:
        bookkeeper_table["current_pk"] = None
        bookkeeper_table["current_page"] = None
        self._upsert_bookkeeper_table(bookkeeper_table)

    def _cycle_upload_page_uploaded(
        self,
        table: Table,
        bookkeeper_table: BookkeeperHixDxhTable,
        context: Context,
    ) -> None:
        self._upsert_bookkeeper_table(bookkeeper_table)

    def _full_upload_done(
        self,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
        context: Context,
    ) -> None:
        bookkeeper_table = cast(BookkeeperHixDxhTable, bookkeeper_table)
        bookkeeper_table["current_pk"] = None
        bookkeeper_table["current_page"] = None
        bookkeeper_table["cycleid"] = self.last_cycle_id + 1
        self._upsert_bookkeeper_table(bookkeeper_table)
