# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Sequence, cast

import backoff
import polars as pl
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context

from ...hooks.db.bq import BigQueryHook
from ...model.bookkeeper import BookkeeperFullUploadTable, BookkeeperTable
from ...model.dbmetadata import Table
from .full_to_bq_base import FullUploadToBigQueryBaseOperator


class BigQueryToOtherGCSOperator(FullUploadToBigQueryBaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_get_bookkeeper_table",
        "sql_upsert_bookkeeper_table",
        "sql_paged_full_upload",
        "sql_get_tables_metadata",
    )

    def __init__(
        self,
        project: str,
        dataset: str,
        sql_paged_full_upload: str = """{% raw %}
select *
from {{ project }}.{{ dataset }}.{{ table }}
LIMIT {{ page_size }} OFFSET {{ page * page_size }}
{% endraw %}""",
        sql_get_tables_metadata: str = """{% raw %}
select table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, clustering_ordinal_position
from {{ project }}.{{ dataset }}.INFORMATION_SCHEMA.COLUMNS
{%- if where_clause %}
{{ where_clause }}
{% endif %}
order by table_catalog, table_schema, table_name, clustering_ordinal_position, ordinal_position
{% endraw %}""",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project = project
        self.dataset = dataset
        self.sql_paged_full_upload = sql_paged_full_upload
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
            where_clause = f"WHERE table_name IN ('" + "','".join(table_names) + "')"
        sql = template.render(
            project=self.project, dataset=self.dataset, where_clause=where_clause
        )

        df = self._query(sql=sql)
        df = df.rename(
            {
                "table_catalog": "database",
                "table_schema": "schema",
                "table_name": "table",
            }
        )

        tables = cast(
            list[Table],
            df.sort(by=["database", "schema", "table", "clustering_ordinal_position"])
            .groupby(
                ["database", "schema", "table"],
            )
            .agg(
                [
                    pl.col("column_name")
                    .filter(pl.col("clustering_ordinal_position").is_not_null())
                    .alias("pks"),
                    pl.col("data_type")
                    .filter(pl.col("clustering_ordinal_position").is_not_null())
                    .alias("pks_type"),
                    pl.col("column_name")
                    .filter(pl.col("clustering_ordinal_position").is_null())
                    .alias("columns"),
                    pl.col("data_type")
                    .filter(pl.col("clustering_ordinal_position").is_null())
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
        else:
            raise Exception(f"Unknown upload strategy: {upload_strategy}")

    def _choose_upload_strategy(
        self,
        table: Table,
        bookkeeper_table: BookkeeperTable,
    ) -> str:
        upload_strategy = "paged_full_upload"

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
                project=table["database"],
                dataset=table["schema"],
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
