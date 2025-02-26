import functools as ft
import json
import uuid
from typing import Sequence, Tuple, cast

import backoff
import google.cloud.bigquery as bq
import polars as pl
from airflow.utils.context import Context
from google.cloud.exceptions import NotFound

from ....hooks.db.connectorx import ConnectorXHook
from ....model.bookkeeper import (
    BookkeeperChangeTrackingTable,
    BookkeeperFullUploadTable,
    BookkeeperTable,
)
from ....model.dbmetadata import Table
from ....utils import AirflowJsonEncoder
from .full_to_bq import SqlServerFullUploadToBigQueryOperator


class SqlServerChangeTrackinToBigQueryOperator(SqlServerFullUploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_get_bookkeeper_tables_referring_to_other_change_tracking_table",
        "sql_get_bookkeeper_table",
        "sql_upsert_bookkeeper_table",
        "sql_paged_full_upload",
        "sql_paged_full_upload_with_cte",
        "sql_topped_full_upload",
        "sql_get_tables_metadata",
        "sql_get_tables_metadata_referring_to_other_change_tracking_table",
        "sql_get_change_tracking_version",
        "sql_bq_merge",
        "sql_change_tracking_table_pk_columns",
        "sql_incremental_upload",
        "sql_cleanup_changetable",
        "sql_bq_deletes_insert",
        "sql_bq_create_deletes_table",
    )

    def __init__(
        self,
        sql_get_bookkeeper_tables_referring_to_other_change_tracking_table: str = """{% raw %}
select database, schema, table, disabled, page_size, current_pk, current_page, version, change_tracking_table
from `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}`
where change_tracking_table is not null
{% endraw %}""",
        sql_get_bookkeeper_table: str = """{% raw %}
select database, schema, table, disabled, page_size, current_pk, current_page, version, change_tracking_table
from `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}`
where database = '{{ database }}' and schema = '{{ schema }}' and table = '{{ table }}'
{% endraw %}""",
        sql_upsert_bookkeeper_table: str = """{% raw %}
MERGE `{{ bookkeeper_dataset }}.{{ bookkeeper_table }}` AS target
USING (SELECT @database as database, @schema as schema, @table as table, @current_pk as current_pk, @current_page as current_page, @version as version) AS source
ON (target.database = source.database and target.schema = source.schema and target.table = source.table)
    WHEN MATCHED THEN
        UPDATE SET current_pk = source.current_pk,
            current_page = source.current_page,
            version = source.version
    WHEN NOT MATCHED THEN
        INSERT (database, schema, table, current_pk, current_page, version)
        VALUES (source.database, source.schema, source.table, source.current_pk, source.current_page, source.version);
{% endraw %}""",
        sql_get_change_tracking_version: str = "select CHANGE_TRACKING_CURRENT_VERSION() as version",
        sql_incremental_upload: str = """{% raw %}
SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, tc.commit_time, {{ pk_columns }}{% if columns %}, {{ columns }}{% endif %}
FROM CHANGETABLE(CHANGES {{ schema }}.{{ change_tracking_table }}, {{ last_synchronization_version }}) AS ct
join sys.dm_tran_commit_table tc on ct.sys_change_version = tc.commit_ts
left outer join {{ schema }}.{{ table }} t with (nolock) on {{ join_on_clause }}
ORDER BY SYS_CHANGE_VERSION
OFFSET 0 ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        sql_bq_merge: str = """{% raw %}
MERGE INTO `{{ dataset }}.{{ table }}` AS t
USING `{{ dataset }}._incremental_{{ table }}` s
ON {{ condition_clause }}
WHEN MATCHED AND s.deleted = true 
    THEN DELETE
{% if update_clause -%} 
WHEN MATCHED 
    THEN UPDATE SET {{ update_clause }}
{%- endif -%}
WHEN NOT MATCHED 
    THEN INSERT({{ insert_columns }})
    VALUES({{ insert_values }})
{% endraw %}""",
        sql_bq_deletes_insert: str = """{% raw %}
INSERT INTO `{{ dataset }}._deletes_{{ table }}`
SELECT i.SYS_CHANGE_VERSION, o.*
FROM `{{ dataset }}._incremental_{{ table }}` i
LEFT OUTER JOIN `{{ dataset }}.{{ table }}` o ON {{ condition_clause }}
WHERE i.deleted = true 
{% endraw %}""",
        sql_bq_create_deletes_table: str = """{% raw %}
CREATE TABLE IF NOT EXISTS `{{ dataset }}._deletes_{{ table }}` 
AS
SELECT CAST(NULL AS INT64) AS SYS_CHANGE_VERSION, t.*
FROM `{{ dataset }}.{{ table }}` t
LIMIT 0;
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
FROM sys.change_tracking_tables tr
INNER JOIN sys.tables t ON t.object_id = tr.object_id
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
INNER JOIN sys.columns col ON col.object_id = t.object_id
LEFT OUTER JOIN sys.indexes pk ON t.object_id = pk.object_id AND pk.is_primary_key = 1 
LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id AND col.column_id = ic.column_id
{%- if where_clause %}
{{ where_clause }}
{% endif %}
ORDER BY [schema], [table], is_pk DESC, ic.index_column_id ASC
{% endraw %}""",
        sql_get_tables_metadata_referring_to_other_change_tracking_table: str = """{% raw %}
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
        sql_change_tracking_table_pk_columns: str = """{% raw %}
SELECT col.name AS pk_col_name
FROM sys.tables t
INNER JOIN sys.indexes pk ON t.object_id = pk.object_id
INNER JOIN sys.index_columns ic ON ic.object_id = pk.object_id AND ic.index_id = pk.index_id
INNER JOIN sys.columns col ON pk.object_id = col.object_id AND col.column_id = ic.column_id
WHERE pk.is_primary_key = 1
    AND t.name = '{{ change_tracking_table }}'
{% endraw %}""",
        sql_cleanup_changetable: str = """{% raw %}
DELETE FROM `{{ dataset }}.CHANGETABLE`
WHERE TABLE = '{{ table }}'
{% endraw %}""",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql_get_bookkeeper_tables_referring_to_other_change_tracking_table = (
            sql_get_bookkeeper_tables_referring_to_other_change_tracking_table
        )
        self.sql_get_bookkeeper_table = sql_get_bookkeeper_table
        self.sql_upsert_bookkeeper_table = sql_upsert_bookkeeper_table
        self.sql_get_change_tracking_version = sql_get_change_tracking_version
        self.sql_incremental_upload = sql_incremental_upload
        self.sql_bq_merge = sql_bq_merge
        self.sql_bq_deletes_insert = sql_bq_deletes_insert
        self.sql_bq_create_deletes_table = sql_bq_create_deletes_table
        self.sql_get_tables_metadata = sql_get_tables_metadata
        self.sql_get_tables_metadata_referring_to_other_change_tracking_table = (
            sql_get_tables_metadata_referring_to_other_change_tracking_table
        )
        self.sql_change_tracking_table_pk_columns = sql_change_tracking_table_pk_columns
        self.sql_cleanup_changetable = sql_cleanup_changetable

        self._airflow_hook = None
        self._airflow_upsert_hook = None
        self.change_tracking_current_version = 0

    def _before_execute(self, context):
        self.change_tracking_current_version = (
            self._get_change_tracking_current_version()
        )

    def _execute_table_ipml(
        self,
        upload_strategy: str,
        table: Table,
        bookkeeper_table: BookkeeperTable,
        context: Context,
    ):
        bookkeeper_table = cast(BookkeeperChangeTrackingTable, bookkeeper_table)
        if upload_strategy == "incremental_upload":
            self._incremental_upload(context, table, bookkeeper_table)
        else:
            self._cleanup_changetable(table)
            super()._execute_table_ipml(
                upload_strategy, table, bookkeeper_table, context
            )
            self._incremental_upload(context, table, bookkeeper_table)

    def _empty_bookkeeper_table_record(self, table: Table) -> BookkeeperTable:
        return cast(
            BookkeeperChangeTrackingTable,
            {
                "database": table["database"],
                "schema": table["schema"],
                "table": table["table"],
                "disabled": None,
                "page_size": None,
                "bulk_upload_page": None,
                "current_pk": None,
                "current_page": None,
                "version": None,
                "change_tracking_table": None,
            },
        )

    def _upsert_bookkeeper_table(
        self,
        bookkeeper_table: BookkeeperTable,
    ):
        bookkeeper_table = cast(BookkeeperChangeTrackingTable, bookkeeper_table)
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
                    "version",
                    "INTEGER",
                    bookkeeper_table["version"],
                ),
            ],
        )
        query_job = client.query(
            query, job_config=job_config, location=self.gcp_location
        )
        query_job.result()

    def _choose_upload_strategy(self, table: Table, bookkeeper_table: BookkeeperTable):
        bookkeeper_table = cast(BookkeeperChangeTrackingTable, bookkeeper_table)
        if bookkeeper_table["version"]:
            upload_strategy = "incremental_upload"
        else:
            upload_strategy = super()._choose_upload_strategy(table, bookkeeper_table)

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def _cleanup_changetable(self, table: Table):
        try:
            jinja_env = self.get_template_env()
            template = jinja_env.from_string(self.sql_cleanup_changetable)
            sql = template.render(
                dataset=self.destination_dataset, table=table["table"]
            )
            self._run_bq_job(
                sql,
                job_id=f"airflow_CHANGETABLE_cleanup_{table['table']}_{str(uuid.uuid4())}",
            )
        except NotFound:  # CHANGETABLE not yet created in BigQuery
            pass

    def _get_tables_referring_to_other_change_tracking_table(self) -> list[str]:
        jinja_env = self.get_template_env()

        hook = self._get_bq_hook()
        template = jinja_env.from_string(
            self.sql_get_bookkeeper_tables_referring_to_other_change_tracking_table
        )
        sql = template.render(
            bookkeeper_dataset=self.bookkeeper_dataset,
            bookkeeper_table=self.bookkeeper_table,
        )
        client = hook.get_client()
        job_config = bq.QueryJobConfig(
            query_parameters=[],
        )
        query_job = client.query(sql, job_config=job_config, location=self.gcp_location)
        rows = query_job.result()
        df = pl.from_arrow(rows.to_arrow())
        return cast(pl.DataFrame, df).get_column("table").to_list()

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_tables_chunk(self, table_names: list[str] | None) -> list[Table]:
        tables_referring_to_other_change_tracking_table = (
            self._get_tables_referring_to_other_change_tracking_table()
        )

        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_get_tables_metadata)

        where_clause = None
        if table_names:
            where_clause = "WHERE t.name IN ('" + "','".join(table_names) + "')"
        sql = template.render(where_clause=where_clause)
        df = self._query(sql=sql)

        if tables_referring_to_other_change_tracking_table:
            if table_names:
                tables_referring_to_other_change_tracking_table = list(
                    set(table_names)
                    & set(tables_referring_to_other_change_tracking_table)
                )

            if tables_referring_to_other_change_tracking_table:
                template = jinja_env.from_string(
                    self.sql_get_tables_metadata_referring_to_other_change_tracking_table
                )
                where_clause = (
                    "WHERE t.name IN ('"
                    + "','".join(tables_referring_to_other_change_tracking_table)
                    + "')"
                )
                sql = template.render(where_clause=where_clause)
                df2 = self._query(sql=sql)
                df = pl.concat([df, df2])

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

    def _incremental_upload(
        self,
        context: Context,
        table: Table,
        bookkeeper_table: BookkeeperChangeTrackingTable,
    ):
        jinja_env = self.get_template_env()

        change_tracking_table = None
        change_tracking_pk_columns = None

        if bookkeeper_table["change_tracking_table"]:
            change_tracking_table = bookkeeper_table["change_tracking_table"]
            change_tracking_pk_columns = self._get_change_tracking_table_pks(
                change_tracking_table
            )
        else:
            change_tracking_table = table["table"]
        if not change_tracking_pk_columns:
            change_tracking_pk_columns = table["pks"]

        pks: list[Tuple[str, str]] = [
            (pk_column, change_tracking_pk_columns[idx])
            for idx, pk_column in enumerate(table["pks"])
        ]
        join_on_clause = ft.reduce(
            lambda a, b: a + " and " + b,
            map(lambda pk: f"t.[{pk[0]}] = ct.[{pk[1]}]", pks),
        )

        page_size = bookkeeper_table["page_size"] or self.page_size
        returned_rows = page_size
        while returned_rows == page_size:
            last_synchronization_version = (
                cast(int, bookkeeper_table["version"])
                if bookkeeper_table["version"]
                else self.change_tracking_current_version
            )

            template = jinja_env.from_string(self.sql_incremental_upload)
            sql = template.render(
                schema=table["schema"],
                table=table["table"],
                change_tracking_table=change_tracking_table,
                pk_columns=", ".join(map(lambda pk: f"ct.[{pk[1]}]", pks)),
                columns=", ".join(
                    map(lambda column: f"t.[{column}]", table["columns"])
                ),
                last_synchronization_version=last_synchronization_version,
                join_on_clause=join_on_clause,
                page_size=self.page_size,
            )
            df: pl.DataFrame = self._query(sql)

            if df.is_empty():
                if not bookkeeper_table["version"]:
                    bookkeeper_table["version"] = self.change_tracking_current_version
                    self._incremental_upload_done(table, bookkeeper_table, context)
                return

            returned_rows = len(df)

            df = self._check_dataframe_for_bigquery_safe_column_names(df)
            last_synchronization_version = df["SYS_CHANGE_VERSION"].max()

            safe_pks = self._generate_bigquery_safe_column_names(table["pks"])
            safe_columns = self._generate_bigquery_safe_column_names(table["columns"])

            insert_columns = ", ".join(map(lambda column: f"`{column}`", safe_pks))
            insert_values = ", ".join(map(lambda column: f"`{column}`", safe_pks))

            if table["columns"]:
                insert_columns = (
                    insert_columns
                    + ", "
                    + ", ".join(map(lambda column: f"`{column}`", safe_columns))
                )
                insert_values = (
                    insert_values
                    + ", "
                    + ", ".join(map(lambda column: f"`{column}`", safe_columns))
                )

            # ----------------------------------------------
            # Upload metadata to CHANGEABLE
            # ----------------------------------------------

            df_changetable = (
                df.with_columns(
                    pl.struct(change_tracking_pk_columns)
                    .map_elements(
                        lambda x: json.dumps(x, cls=AirflowJsonEncoder),
                        return_dtype=pl.Utf8,
                    )
                    .alias("KEY")
                )
                .select(
                    ["SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "commit_time", "KEY"]
                )
                .with_columns(pl.lit(table["table"]).alias("TABLE"))
            )

            self._upload_parquet(
                df=df_changetable,
                object_name=f"{self.bucket_dir}/CHANGETABLE/{table['table']}/{table['table']}_{last_synchronization_version}.parquet",
            )

            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/CHANGETABLE/{table['table']}/{table['table']}_{last_synchronization_version}.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table="CHANGETABLE",
                cluster_fields=["TABLE", "SYS_CHANGE_VERSION"],
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
            )

            # ----------------------------------------------
            # Upload to _incremental_ table
            # ----------------------------------------------
            df = df.unique(subset=change_tracking_pk_columns, keep="last")

            for pk in (
                pk for pk in pks if pk[0] != pk[1]
            ):  # rename the change_tracking_table pk's if table not equals change_tracking_table
                df = df.rename({pk[1]: pk[0]})
            df = df.with_columns(
                pl.when(pl.col("SYS_CHANGE_OPERATION") == "D")
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("deleted")
            )  # add deleted column
            df.drop_in_place("SYS_CHANGE_OPERATION")
            # df.drop_in_place("SYS_CHANGE_VERSION")
            df.drop_in_place("commit_time")

            self._upload_parquet(
                df=df,
                object_name=f"{self.bucket_dir}/{table['table']}/incremental/{table['table']}_{last_synchronization_version}.parquet",
                table_metadata=table,
            )

            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table']}/incremental/{table['table']}_{last_synchronization_version}.parquet"
                ],
                destination_project_id=self.destination_project_id,
                destination_dataset=self.destination_dataset,
                destination_table=f"_incremental_{table['table']}",
                cluster_fields=self._get_cluster_fields(table),
            )

            # ----------------------------------------------
            # Store deleted records in seperate table
            # ----------------------------------------------
            hook = self._get_bq_hook()
            client = hook.get_client()
            deletes_table_exists = False
            try:
                client.get_table(
                    f"{self.destination_project_id}.{self.destination_dataset}._deletes_{table['table']}"
                )  # Make an API request.
                deletes_table_exists = True
            except NotFound:
                pass

            if not deletes_table_exists:
                table_exists = False
                try:
                    client.get_table(
                        f"{self.destination_project_id}.{self.destination_dataset}.{table['table']}"
                    )  # Make an API request.
                    table_exists = True
                except NotFound:
                    pass  # the first time the base table wil not exists, so we skip the creation of the deletes table
                if table_exists:
                    template = jinja_env.from_string(self.sql_bq_create_deletes_table)
                    sql = template.render(
                        dataset=self.destination_dataset,
                        table=table["table"],
                    )
                    self._run_bq_job(
                        sql,
                        job_id=f"airflow_{table['schema']}_create_deletes_{table['table']}_{str(uuid.uuid4())}",
                    )
                    deletes_table_exists = True

            if deletes_table_exists:
                template = jinja_env.from_string(self.sql_bq_deletes_insert)
                sql = template.render(
                    dataset=self.destination_dataset,
                    table=table["table"],
                    condition_clause=" and ".join(
                        map(
                            lambda column: f"i.`{column}` = o.`{column}`",
                            safe_pks or safe_columns,
                        )
                    ),
                    insert_columns=insert_columns,
                    insert_values=insert_values,
                )
                self._run_bq_job(
                    sql,
                    job_id=f"airflow_{table['schema']}_deletes_{table['table']}_{str(uuid.uuid4())}",
                )

            # ----------------------------------------------
            # Merge changes to table
            # ----------------------------------------------
            template = jinja_env.from_string(self.sql_bq_merge)
            sql = template.render(
                dataset=self.destination_dataset,
                table=table["table"],
                condition_clause=" and ".join(
                    map(lambda column: f"t.`{column}` = s.`{column}`", safe_pks)
                ),
                update_clause=", ".join(
                    map(lambda column: f"t.`{column}` = s.`{column}`", safe_columns)
                ),
                insert_columns=insert_columns,
                insert_values=insert_values,
            )
            self._run_bq_job(
                sql,
                job_id=f"airflow_{table['schema']}_{table['table']}_{str(uuid.uuid4())}",
            )

            # ----------------------------------------------
            # Cleanup
            # ----------------------------------------------
            self._delete_bq_table(
                dataset_table=f"{self.destination_dataset}._incremental_{table['table']}",
            )

            if returned_rows == page_size:
                bookkeeper_table["version"] = cast(int, last_synchronization_version)
                self._incremental_upload_done(table, bookkeeper_table, context)

        bookkeeper_table["version"] = self.change_tracking_current_version
        self._incremental_upload_done(table, bookkeeper_table, context)

    def _incremental_upload_done(
        self,
        table: Table,
        bookkeeper_table: BookkeeperFullUploadTable,
        context: Context,
    ) -> None:
        bookkeeper_table["current_pk"] = None
        bookkeeper_table["current_page"] = None
        self._upsert_bookkeeper_table(bookkeeper_table)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_change_tracking_table_pks(self, change_tracking_table: str) -> list[str]:
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_change_tracking_table_pk_columns)
        sql = template.render(
            change_tracking_table=change_tracking_table,
        )
        df = self._query(sql)
        return list(df["pk_col_name"])

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_change_tracking_current_version(self) -> int:
        hook = cast(ConnectorXHook, self._get_db_hook())
        df = cast(pl.DataFrame, hook.run(self.sql_get_change_tracking_version))
        return df["version"][0]
