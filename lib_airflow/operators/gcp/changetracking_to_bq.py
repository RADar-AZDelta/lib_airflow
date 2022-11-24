import functools as ft
import uuid
from typing import Dict, List, Optional, Sequence, Tuple, cast

import backoff
import polars as pl
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.context import Context
from lib_airflow.hooks.db.connectorx import ConnectorXHook

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
        # tables: List[ChangeTrackingTable],
        connectorx_airflow_conn_id: str,
        odbc_airflow_conn_id: str,
        sql_current_sync_version: str,
        sql_upsert_current_sync_version: str,
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
        # self.tables = tables
        self.connectorx_airflow_conn_id = connectorx_airflow_conn_id
        self.odbc_airflow_conn_id = odbc_airflow_conn_id
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
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            page=page,
        )

    def _full_upload_done(self, table: Table) -> None:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            page=None,
            current_identity=None,
        )

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
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            current_identity=identity,
            page=page,
        )

    def _full_upload_with_identity_pk_done(self, table: Table) -> None:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            page=None,
            current_identity=None,
        )

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
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            page=page,
            current_single_nvarchar_pk=pk,
        )

    def _full_upload_with_single_nvarchar_pk_done(self, table: Table) -> None:
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        self._upsert_current_sync_version(
            table,
            self.change_tracking_current_version,
            current_sync_version,
            page=None,
            current_single_nvarchar_pk=None,
        )

    def _incremental_upload(
        self,
        context: Context,
        table: Table,
    ):
        jinja_env = self.get_template_env()

        change_tracking_table = None
        change_tracking_pk_columns = None
        current_sync_version = self.current_sync_versions[
            f"{table['database_name']}_{table['schema_name']}_{table['table_name']}"
        ]
        if current_sync_version and current_sync_version["change_tracking_table"]:
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

        if self._check_parquet(  # IS THIS NECESSARY??????
            f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/incremental/{table['table_name']}_{last_synchronization_version}.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_project_dataset}._incremental_{table['table_name']}",
                cluster_fields=self._get_cluster_fields(table),
            )

            template = jinja_env.from_string(self.sql_bq_merge)

            insert_columns = ", ".join(
                map(
                    lambda column: f"`{column}`",
                    self._rename_bigquery_column_names(table["pks"]),
                )
            )
            insert_values = ", ".join(
                map(
                    lambda column: f"`{column}`",
                    self._rename_bigquery_column_names(table["pks"]),
                )
            )

            if table["columns"]:
                insert_columns = (
                    insert_columns
                    + ", "
                    + ", ".join(
                        map(
                            lambda column: f"`{column}`",
                            self._rename_bigquery_column_names(table["columns"]),
                        )
                    )
                )
                insert_values = (
                    insert_values
                    + ", "
                    + ", ".join(
                        map(
                            lambda column: f"`{column}`",
                            self._rename_bigquery_column_names(table["columns"]),
                        )
                    )
                )

            sql = template.render(
                dataset=self.destination_project_dataset,
                table=table["table_name"],
                condition_clause=" and ".join(
                    map(
                        lambda column: f"t.`{column}` = s.`{column}`",
                        self._rename_bigquery_column_names(table["pks"]),
                    )
                ),
                update_clause=", ".join(
                    map(
                        lambda column: f"t.`{column}` = s.`{column}`",
                        self._rename_bigquery_column_names(table["columns"]),
                    )
                ),
                insert_columns=insert_columns,
                insert_values=insert_values,
            )
            self._run_bq_job(
                sql,
                job_id=f"airflow_{table['schema_name']}_{table['table_name']}_{str(uuid.uuid4())}",
            )
            self._delete_bq_table(
                dataset_table=f"{self.destination_project_dataset}._incremental_{table['table_name']}",
            )

            self._upsert_current_sync_version(
                table,
                self.change_tracking_current_version,
                current_sync_version=None,
                page=None,
                current_identity=None,
            )

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
        table: Table,
        change_tracking_current_version: int,
        current_sync_version: Optional[AirflowSyncChangeTrackingVersion],
        version: Optional[int] = None,
        page: Optional[int] = None,
        current_identity: Optional[int] = None,
        current_single_nvarchar_pk: Optional[str] = None,
    ):
        jinja_env = self.get_template_env()

        hook = self._get_airflow_upsert_hook()
        template = jinja_env.from_string(self.sql_upsert_current_sync_version)
        sql = template.render()
        if not version:
            sync_version = (
                current_sync_version["version"] if current_sync_version else None
            )
            version = sync_version or change_tracking_current_version
        hook.run(
            sql,
            parameters=[
                table["database_name"],
                table["schema_name"],
                table["table_name"],
                version,
                page,
                current_identity,
                current_single_nvarchar_pk,
            ],
        )

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_current_sync_version(
        self, table: Table
    ) -> Optional[AirflowSyncChangeTrackingVersion]:
        jinja_env = self.get_template_env()

        hook = ConnectorXHook(connectorx_conn_id=self.connectorx_airflow_conn_id)
        template = jinja_env.from_string(self.sql_current_sync_version)
        query = template.render(
            {
                "params": {
                    "database_name": table["database_name"],
                    "schema_name": table["schema_name"],
                    "table_name": table["table_name"],
                }
            }
        )
        df = hook.get_polars_dataframe(query=query)
        list = df.to_dicts()
        return cast(AirflowSyncChangeTrackingVersion, list[0]) if list else None

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

    def _get_airflow_upsert_hook(self) -> OdbcHook:
        if not self._airflow_upsert_hook:
            self._airflow_upsert_hook = OdbcHook(odbc_conn_id=self.odbc_airflow_conn_id)
        return self._airflow_upsert_hook

    def _get_airflow_hook(self) -> ConnectorXHook:
        if not self._airflow_hook:
            self._airflow_hook = ConnectorXHook(
                connectorx_conn_id=self.connectorx_airflow_conn_id
            )
        return self._airflow_hook
