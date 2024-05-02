import json
import traceback
from abc import ABC, abstractmethod
from typing import Callable, Iterable, List, Sequence, Union, cast

import backoff
import google.cloud.bigquery as bq
import polars as pl
from airflow.utils.context import Context
from airflow.utils.email import send_email

from ...model.bookkeeper import BookkeeperFullUploadTable, BookkeeperTable
from ...model.dbmetadata import Table
from .query_and_upload_to_bq import QueryAndUploadToBigQueryOperator


class FullUploadToBigQueryBaseOperator(QueryAndUploadToBigQueryOperator, ABC):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_get_bookkeeper_table",
        "sql_upsert_bookkeeper_table",
    )

    def __init__(
        self,
        bookkeeper_dataset: str,
        bookkeeper_table: str,
        destination_dataset: str,
        func_get_table_names: Callable[[str], list[str]] | None = None,
        chunk: int = 0,
        nbr_of_chunks: int = 1,
        sql_get_bookkeeper_table: str = """{% raw %}
select database, schema, table, disabled, page_size, current_pk, current_page
from `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}`
where database = '{{ database }}' and schema = '{{ schema }}' and table = '{{ table }}'
{% endraw %}""",
        sql_upsert_bookkeeper_table: str = """{% raw %}
MERGE `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}` AS target
USING (SELECT @database as database, @schema as schema, @table as table, @current_pk as current_pk, @current_page as current_page) AS source
ON (target.database = source.database and target.schema = source.schema and target.table = source.table)
    WHEN MATCHED THEN
        UPDATE SET current_pk = source.current_pk,
            current_page = source.current_page
    WHEN NOT MATCHED THEN
        INSERT (database, schema, table, current_pk, current_page)
        VALUES (source.database, source.schema, source.table, source.current_pk, source.current_page);
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
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.destination_dataset = destination_dataset
        self.to_email_on_error = to_email_on_error
        self.func_before_execute_table = func_before_execute_table
        self.func_after_execute_table = func_after_execute_table

    @abstractmethod
    def _get_tables_chunk(self, table_names: list[str] | None) -> list[Table]:
        pass

    def execute(self, context):
        self._before_execute(context)
        table_names = (
            self.func_get_table_names(self.source_db_conn_id)
            if self.func_get_table_names
            else None
        )
        tables: list[Table] = self._get_tables_chunk(table_names)

        for table in tables:
            str_error = None
            try:
                bookkeeper_table = self._get_bookkeeper_table(table)
                if bookkeeper_table and bookkeeper_table["disabled"]:
                    self.log.info("Table %s is disabled!", table["table"])
                    continue
                if self.func_before_execute_table:
                    continue_table_execute = self.func_before_execute_table(
                        table, context
                    )
                    if not continue_table_execute:
                        self.log.info("Skipping table %s", table["table"])
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
        self._execute_table_ipml(upload_strategy, table, bookkeeper_table, context)

    @abstractmethod
    def _execute_table_ipml(
        self,
        upload_strategy: str,
        table: Table,
        bookkeeper_table: BookkeeperTable,
        context: Context,
    ):
        pass

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
                hook.delete(self.bucket, object_name)

    @abstractmethod
    def _choose_upload_strategy(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
    ) -> str:
        pass

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
            if not table["pks_type"][i]
            in [
                "float",
                "numeric",
                "binary",
            ]  # cluster fields cannot be float or numeric or binary
        ][:4]
        return cluster_fields
