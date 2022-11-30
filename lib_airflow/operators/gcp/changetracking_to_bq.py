import functools as ft
import uuid
from typing import Dict, List, Optional, Sequence, Tuple, cast

import backoff
import google.cloud.bigquery as bq
import polars as pl
from airflow.utils.context import Context

from ...model.changetracking import AirflowSyncChangeTrackingVersion
from ...model.dbmetadata import Table
from . import FullUploadToBigQueryOperator


class ChangeTrackinToBigQueryOperator(FullUploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_change_tracking_current_version",
        "sql_current_sync_version",
        "sql_upsert_current_sync_version",
        "sql_full_upload",
        "sql_full_upload_with_identity_pk",
        "max_identity_sql",
        "sql_incremental_upload",
        "sql_bq_merge",
        "sql_change_tracking_table_pk_columns",
        "sql_full_upload_with_single_nvarchar_pk",
    )

    def __init__(
        self,
        bookkeeper_dataset: str,
        bookkeeper_table: str,
        sql_current_sync_version: str = """{% raw %}
select database, schema, table, page_size, change_tracking_table, disabled, version, bulk_upload_page, current_identity_value, current_single_nvarchar_pk
from {{ bookkeeper_dataset }}.{{ bookkeeper_table }}
where database = '{{ database_name }}' and schema = '{{ schema_name }}' and table = '{{ table_name }}'
{% endraw %}""",
        sql_upsert_current_sync_version: str = """{% raw %}
MERGE {{ bookkeeper_dataset }}.{{ bookkeeper_table }} AS target
USING (SELECT @database as database, @schema as schema, @table as table, @version as version, @bulk_upload_page as bulk_upload_page, @current_identity_value as current_identity_value, @current_single_nvarchar_pk as current_single_nvarchar_pk) AS source
ON (target.database = source.database and target.schema = source.schema and target.table = source.table)
    WHEN MATCHED THEN
        UPDATE SET version = source.version,
            bulk_upload_page = source.bulk_upload_page,
            current_identity_value = source.current_identity_value,
            current_single_nvarchar_pk = source.current_single_nvarchar_pk
    WHEN NOT MATCHED THEN
        INSERT (database, schema, table, version, bulk_upload_page, current_identity_value, current_single_nvarchar_pk)
        VALUES (source.database, source.schema, source.table, source.version, source.bulk_upload_page, source.current_identity_value, source.current_single_nvarchar_pk);
{% endraw %}""",
        sql_change_tracking_current_version: str = "select CHANGE_TRACKING_CURRENT_VERSION() as version",
        sql_incremental_upload: str = """{% raw %}
SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, {{ pk_columns }}{% if columns %}, {{ columns }}{% endif %}
FROM CHANGETABLE(CHANGES {{ schema }}.{{ change_tracking_table }}, {{ last_synchronization_version }}) AS ct
left outer join {{ schema }}.{{ table }} t with (nolock) on {{ join_on_clause }}
ORDER BY SYS_CHANGE_VERSION
OFFSET 0 ROWS
FETCH NEXT {{ page_size }} ROWS ONLY
{% endraw %}""",
        sql_bq_merge: str = """{% raw %}
MERGE INTO {{ dataset }}.{{ table }} AS t
USING {{ dataset }}._incremental_{{ table }} s
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
        sql_change_tracking_table_pk_columns: str = """{% raw %}
select col.name as pk_col_name
from sys.tables t
inner join sys.indexes pk on t.object_id = pk.object_id
inner join sys.index_columns ic on ic.object_id = pk.object_id and ic.index_id = pk.index_id
inner join sys.columns col on pk.object_id = col.object_id and col.column_id = ic.column_id
where pk.is_primary_key = 1
	and t.name = '{{ change_tracking_table }}'
{% endraw %}""",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bookkeeper_dataset = bookkeeper_dataset
        self.bookkeeper_table = bookkeeper_table
        self.sql_change_tracking_current_version = sql_change_tracking_current_version
        self.sql_current_sync_version = sql_current_sync_version
        self.sql_upsert_current_sync_version = sql_upsert_current_sync_version
        self.sql_incremental_upload = sql_incremental_upload
        self.sql_bq_merge = sql_bq_merge
        self.sql_change_tracking_table_pk_columns = sql_change_tracking_table_pk_columns

        self._airflow_hook = None
        self._airflow_upsert_hook = None
        self.change_tracking_current_version = 0
        self.current_sync_versions: Dict[
            str, Optional[AirflowSyncChangeTrackingVersion]
        ] = {}

    def _before_execute(self, context):
        self.change_tracking_current_version = (
            self._get_change_tracking_current_version()
        )

    def execute_table(
        self,
        table: Table,
        context: Context,
    ):
        self.log.info("Table: %s", table["table_name"])
        current_sync_version = self._get_current_sync_version(table)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version
        if current_sync_version and current_sync_version["disabled"]:
            return
        upload_strategy = self._choose_upload_strategy(table)
        if upload_strategy == "full_upload":
            self._full_upload(context, table)
        elif upload_strategy == "full_upload_with_identity_pk":
            self._full_upload_with_identity_pk(context, table)
        elif upload_strategy == "full_upload_with_single_nvarchar_pk":
            self._full_upload_with_single_nvarchar_pk(context, table)
        elif upload_strategy == "incremental_upload":
            self._incremental_upload(context, table)
        else:
            raise Exception(f"Unknown upload strategy: {upload_strategy}")

    def get_page_size(self, table: Table):
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        return (
            current_sync_version["page_size"]
            if current_sync_version and current_sync_version["page_size"]
            else self.page_size
        )

    def _choose_upload_strategy(self, table: Table):
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if (
            not current_sync_version
            or current_sync_version["bulk_upload_page"]
            or current_sync_version["current_identity_value"]
            or current_sync_version["current_single_nvarchar_pk"]
            or (
                not current_sync_version["bulk_upload_page"]
                and not current_sync_version["current_identity_value"]
                and not current_sync_version["version"]
                and not current_sync_version["current_single_nvarchar_pk"]
            )
        ):
            if table["is_identity"]:
                upload_strategy = "full_upload_with_identity_pk"
            elif len(table["pks"]) == 1 and table["pks_type"][0] == "nvarchar":
                upload_strategy = "full_upload_with_single_nvarchar_pk"
            else:
                upload_strategy = "full_upload"
        else:
            upload_strategy = "incremental_upload"

        self.log.info("Upload strategy: %s", upload_strategy)
        return upload_strategy

    def _full_upload_get_start_page(self, table: Table) -> int:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        return (
            cast(int, current_sync_version["bulk_upload_page"])
            if current_sync_version
            else 0
        )

    def _full_upload_page_uploaded(self, table: Table, page: int) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]

        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["bulk_upload_page"] = page
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

    def _full_upload_done(
        self,
        table: Table,
        context: Context,
    ) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["bulk_upload_page"] = None
        current_sync_version["version"] = self.change_tracking_current_version
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

        self._incremental_upload(context, table)

    def _full_upload_with_identity_pk_get_start_identity(
        self,
        context: Context,
        table: Table,
    ) -> Tuple[int, int]:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        return (
            cast(int, current_sync_version["current_identity_value"])
            if current_sync_version and current_sync_version["current_identity_value"]
            else 0
        ), (
            cast(int, current_sync_version["bulk_upload_page"])
            if current_sync_version and current_sync_version["bulk_upload_page"]
            else 0
        )

    def _full_upload_with_identity_pk_page_uploaded(
        self, table: Table, identity: int, page: int
    ) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]

        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["current_identity_value"] = identity
        current_sync_version["bulk_upload_page"] = page
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

    def _full_upload_with_identity_pk_done(
        self, table: Table, context: Context
    ) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["current_identity_value"] = None
        current_sync_version["bulk_upload_page"] = None
        current_sync_version["version"] = self.change_tracking_current_version
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

        self._incremental_upload(context, table)

    def _full_upload_with_single_nvarchar_pk_get_start_pk(
        self,
        context: Context,
        table: Table,
    ) -> Tuple[str, int]:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        return (
            cast(str, current_sync_version["current_single_nvarchar_pk"])
            if current_sync_version
            and current_sync_version["current_single_nvarchar_pk"]
            else ""
        ), (
            cast(int, current_sync_version["bulk_upload_page"])
            if current_sync_version and current_sync_version["bulk_upload_page"]
            else 0
        )

    def _full_upload_with_single_nvarchar_pk_page_uploaded(
        self, table: Table, pk: str, page: int
    ) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]

        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["current_single_nvarchar_pk"] = pk
        current_sync_version["bulk_upload_page"] = page
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

    def _full_upload_with_single_nvarchar_pk_done(
        self, table: Table, context: Context
    ) -> None:
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }

        current_sync_version["current_single_nvarchar_pk"] = None
        current_sync_version["bulk_upload_page"] = None
        current_sync_version["version"] = self.change_tracking_current_version
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

        self._incremental_upload(context, table)

    def _incremental_upload(
        self,
        context: Context,
        table: Table,
    ):
        jinja_env = self.get_template_env()

        change_tracking_table = None
        change_tracking_pk_columns = None
        current_sync_version: Optional[
            AirflowSyncChangeTrackingVersion
        ] = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if not current_sync_version:
            current_sync_version = {
                "database": table["database_name"],
                "schema": table["schema_name"],
                "table": table["table_name"],
                "page_size": None,
                "change_tracking_table": None,
                "disabled": None,
                "version": None,
                "bulk_upload_page": None,
                "current_identity_value": None,
                "current_single_nvarchar_pk": None,
            }
        if current_sync_version["change_tracking_table"]:
            change_tracking_table = current_sync_version["change_tracking_table"]
            change_tracking_pk_columns = self._get_change_tracking_table_pks(
                change_tracking_table
            )
        else:
            change_tracking_table = table["table_name"]
        if not change_tracking_pk_columns:
            change_tracking_pk_columns = table["pks"]

        pks: List[Tuple[str, str]] = [
            (pk_column, change_tracking_pk_columns[idx])
            for idx, pk_column in enumerate(table["pks"])
        ]
        join_on_clause = ft.reduce(
            lambda a, b: a + " and " + b,
            map(lambda pk: f"t.[{pk[0]}] = ct.[{pk[1]}]", pks),
        )

        last_synchronization_version = (
            cast(int, current_sync_version["version"]) if current_sync_version else 0
        )

        template = jinja_env.from_string(self.sql_incremental_upload)
        sql = template.render(
            schema=table["schema_name"],
            table=table["table_name"],
            change_tracking_table=change_tracking_table,
            pk_columns=", ".join(map(lambda pk: f"ct.[{pk[1]}]", pks)),
            columns=", ".join(map(lambda column: f"t.[{column}]", table["columns"])),
            last_synchronization_version=last_synchronization_version,
            join_on_clause=join_on_clause,
            page_size=self.page_size,
        )
        df = self._query(sql)

        if len(df) == 0:
            return 0

        df = self._check_dataframe_for_bigquery_safe_column_names(df)
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

        returned_rows = self._upload_parquet(
            df=df,
            object_name=f"{self.bucket_dir}/{table['table_name']}/incremental/{table['table_name']}_{last_synchronization_version}.parquet",
            table_metadata=table,
        )

        self._load_parquets_in_bq(
            source_uris=[
                f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/incremental/{table['table_name']}_{last_synchronization_version}.parquet"
            ],
            destination_project_dataset_table=f"{self.destination_dataset}._incremental_{table['table_name']}",
            cluster_fields=self._get_cluster_fields(table),
        )

        template = jinja_env.from_string(self.sql_bq_merge)

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

        sql = template.render(
            dataset=self.destination_dataset,
            table=table["table_name"],
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
            job_id=f"airflow_{table['schema_name']}_{table['table_name']}_{str(uuid.uuid4())}",
        )
        self._delete_bq_table(
            dataset_table=f"{self.destination_dataset}._incremental_{table['table_name']}",
        )

        current_sync_version["version"] = self.change_tracking_current_version
        self._upsert_current_sync_version(current_sync_version)
        self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ] = current_sync_version

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_change_tracking_table_pks(self, change_tracking_table: str) -> List[str]:
        jinja_env = self.get_template_env()
        template = jinja_env.from_string(self.sql_change_tracking_table_pk_columns)
        sql = template.render(
            change_tracking_table=change_tracking_table,
        )
        df = self._query(sql)
        return df.get_column("pk_col_name").to_list()

    def _upsert_current_sync_version(
        self,
        current_sync_version: AirflowSyncChangeTrackingVersion,
    ):
        jinja_env = self.get_template_env()

        hook = self._get_bq_hook()
        template = jinja_env.from_string(self.sql_upsert_current_sync_version)
        query = template.render(
            bookkeeper_dataset=self.bookkeeper_dataset,
            bookkeeper_table=self.bookkeeper_table,
        )
        client = hook.get_client()
        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter(
                    "database", "STRING", current_sync_version["database"]
                ),
                bq.ScalarQueryParameter(
                    "schema", "STRING", current_sync_version["schema"]
                ),
                bq.ScalarQueryParameter(
                    "table", "STRING", current_sync_version["table"]
                ),
                bq.ScalarQueryParameter(
                    "version", "INTEGER", current_sync_version["version"]
                ),
                bq.ScalarQueryParameter(
                    "bulk_upload_page",
                    "INTEGER",
                    current_sync_version["bulk_upload_page"],
                ),
                bq.ScalarQueryParameter(
                    "current_identity_value",
                    "INTEGER",
                    current_sync_version["current_identity_value"],
                ),
                bq.ScalarQueryParameter(
                    "current_single_nvarchar_pk",
                    "STRING",
                    current_sync_version["current_single_nvarchar_pk"],
                ),
            ],
        )
        query_job = client.query(
            query, job_config=job_config, location=self.gcp_location
        )
        result = query_job.result()

    def _get_current_sync_version(
        self, table: Table
    ) -> Optional[AirflowSyncChangeTrackingVersion]:
        jinja_env = self.get_template_env()

        hook = self._get_bq_hook()
        template = jinja_env.from_string(self.sql_current_sync_version)
        query = template.render(
            bookkeeper_dataset=self.bookkeeper_dataset,
            bookkeeper_table=self.bookkeeper_table,
            database_name=table["database_name"],
            schema_name=table["schema_name"],
            table_name=table["table_name"],
        )
        client = hook.get_client()
        job_config = bq.QueryJobConfig(
            query_parameters=[],
        )
        query_job = client.query(
            query, job_config=job_config, location=self.gcp_location
        )
        rows = query_job.result()
        records = [dict(row.items()) for row in rows]
        return cast(AirflowSyncChangeTrackingVersion, records[0]) if records else None

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_change_tracking_current_version(self) -> int:
        hook = self._get_db_hook()
        df = hook.get_polars_dataframe(query=self.sql_change_tracking_current_version)
        return df["version"][0]
