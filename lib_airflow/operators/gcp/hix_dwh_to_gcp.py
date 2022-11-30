import uuid
from typing import Optional, Sequence, cast

import backoff
import polars as pl
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.job import QueryJob
from google.cloud.bigquery.retry import DEFAULT_RETRY as BQ_DEFAULT_RETRY

from libs.lib_airflow.lib_airflow.hooks.db.connectorx import ConnectorXHook
from libs.lib_airflow.lib_airflow.operators.gcp import FullUploadToBigQueryOperator

from ...model.dbmetadata import Table


class HixDwhToBigQueryOperator(FullUploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "page_size",
        "sql_full_upload",
        "sql_full_upload_with_identity_pk",
        "sql_full_upload_with_single_nvarchar_pk",
        "sql_last_synced_table_cycle_id",
        "sql_upsert_last_synced_table_cycle_id",
    )

    def __init__(
        self,
        connectorx_bookkeeper_conn_id: str,
        odbc_bookkeeper_conn_id: str,
        sql_last_synced_table_cycle_id: str,
        sql_upsert_last_synced_table_cycle_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.connectorx_bookkeeper_conn_id = connectorx_bookkeeper_conn_id
        self.odbc_bookkeeper_conn_id = odbc_bookkeeper_conn_id
        self.sql_last_synced_table_cycle_id = sql_last_synced_table_cycle_id
        self.sql_upsert_last_synced_table_cycle_id = (
            sql_upsert_last_synced_table_cycle_id
        )

        self.last_cycle_id = 0
        self._airflow_hook = None
        self._airflow_upsert_hook = None

    def _before_execute(self, context):
        self.last_cycle_id = self._get_last_cycle_id()

    def execute_table(
        self,
        table: Table,
        context: Context,
    ):
        self.log.info(f"Table: {table['table_name']}")

        cycle_id = self._get_last_synced_table_cycle_id(table)
        if not cycle_id:
            cycle_id = self._get_table_cycle_id(table)

        if cycle_id == 0:
            self._full_upload(table)
        else:
            cycle_id += 1

            while cycle_id <= self.last_cycle_id:
                self._append_cycle(table, cycle_id)
                self._upsert_current_cycleid(table, cycle_id)
                cycle_id += 1

    def _get_last_cycle_id(self) -> int:
        df = self._query(
            "select max(cycleid) as last_cycle_id from hdw.CycleLog with (nolock) where EndDateUtc <= GETDATE()"
        )
        return df["last_cycle_id"][0]

    def _get_table_cycle_id(self, table: Table) -> int:
        sql = f"select max(HdwCycleId) as max_cycleid from {self.destination_dataset}.{table['table_name']}"
        job_id = f"max_cycleid_{table['schema_name']}_{table['table_name']}_{str(uuid.uuid4())}"

        try:
            query_job = cast(QueryJob, self._run_bq_job(sql=sql, job_id=job_id))
            df = pl.from_arrow(query_job.to_arrow())
            max_cycleid = cast(int, df["max_cycleid"][0])
            return max_cycleid
        except NotFound as e:
            return 0

    def _full_upload(self, table: Table):
        jinja_env = self.get_template_env()

        pk_is_shakey = any(pk for pk in table["pks"] if pk.endswith("Sha1Key"))
        if pk_is_shakey:
            table_sequel = (
                "Link"
                if table["table_name"].endswith("LinkSat")
                or table["table_name"].endswith("Link")
                else ""
            )
            pk_name = f"Hdw{table_sequel}Sha1Key"
            pk_value = "0x0"

        else:
            # self.log.warn(
            #     f"Table {table['schema_name']}.{table['table_name']} has no Sha1Key key!"
            # )
            pk_name = [pk for pk in table["pks"] if pk != "HdwLoadDateUtc"][0]
            pk_value = "''"

        page = 0
        page_size = self.get_page_size(table)
        returned_rows = page_size
        while returned_rows == page_size:
            template = jinja_env.from_string(
                """
select top {{ page_size }} *
from {{ schema }}.{{ table }}
where {{ pk_name }} >= {{ pk_value }};
"""
            )
            sql = template.render(
                schema=table["schema_name"],
                table=table["table_name"],
                pk_name=pk_name,
                pk_value=pk_value,
                page_size=self.page_size,
            )
            df = self._query(sql)
            returned_rows = len(df)
            if returned_rows:
                if pk_is_shakey:
                    pk_value = "0x" + "".join(
                        "{:02x}".format(x) for x in df[pk_name][-1]  # get last sha1key
                    )
                else:
                    pk_value = df[pk_name][-1]  # get last value
                df = df.filter(pl.col("HdwCycleId") <= self.last_cycle_id)
                df = self._check_dataframe_for_bigquery_safe_column_names(df)
                self._upload_parquet(
                    df,
                    object_name=f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_{page}.parquet",
                    table_metadata=table,
                )
                page += 1

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/full/{table['table_name']}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table_name']}",
                # cluster_fields=self._get_cluster_fields(table), # "Clustering is not supported on non-orderable column 'HdwLinkSha1Key' of type 'STRUCT<list ARRAY<STRUCT<item INT64>>>'"
                write_disposition="WRITE_TRUNCATE",
            )

    def _append_cycle(self, table: Table, cycle_id: int):
        self.log.info(f"Append cycle {cycle_id} to table {table['table_name']}")
        jinja_env = self.get_template_env()

        pk_is_shakey = any(pk for pk in table["pks"] if pk.endswith("Sha1Key"))
        if pk_is_shakey:
            table_sequel = (
                "Link"
                if table["table_name"].endswith("LinkSat")
                or table["table_name"].endswith("Link")
                else ""
            )
            pk_name = f"Hdw{table_sequel}Sha1Key"

        else:
            pk_name = [pk for pk in table["pks"] if pk != "HdwLoadDateUtc"][0]

        page = self._full_upload_get_start_page(table)
        page_size = self.get_page_size(table)
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
                schema=table["schema_name"],
                table=table["table_name"],
                pk_name=pk_name,
                cycle_id=cycle_id,
                page_size=self.page_size,
                page=page,
            )
            returned_rows = self._query_to_parquet_and_upload(
                sql=sql,
                object_name=f"{self.bucket_dir}/{table['table_name']}/cycle_{cycle_id}/{table['table_name']}_{cycle_id}_{page}.parquet",
                table_metadata=table,
            )
            page += 1

        if page == 1 and returned_rows == 0:
            return

        if self._check_parquet(
            f"{self.bucket_dir}/{table['table_name']}/cycle_{cycle_id}/{table['table_name']}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table['table_name']}/cycle_{cycle_id}/{table['table_name']}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table['table_name']}",
                # cluster_fields=self._get_cluster_fields(table), # "Clustering is not supported on non-orderable column 'HdwLinkSha1Key' of type 'STRUCT<list ARRAY<STRUCT<item INT64>>>'"
                write_disposition="WRITE_APPEND",
                schema_update_options=["ALLOW_FIELD_ADDITION"],
            )

    def _get_airflow_upsert_hook(self) -> OdbcHook:
        if not self._airflow_upsert_hook:
            self._airflow_upsert_hook = OdbcHook(
                odbc_conn_id=self.odbc_bookkeeper_conn_id
            )
        return self._airflow_upsert_hook

    def _get_airflow_hook(self) -> ConnectorXHook:
        if not self._airflow_hook:
            self._airflow_hook = ConnectorXHook(
                connectorx_conn_id=self.connectorx_bookkeeper_conn_id
            )
        return self._airflow_hook

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _get_last_synced_table_cycle_id(self, table: Table) -> Optional[int]:
        jinja_env = self.get_template_env()

        hook = ConnectorXHook(connectorx_conn_id=self.connectorx_bookkeeper_conn_id)
        template = jinja_env.from_string(self.sql_last_synced_table_cycle_id)
        query = template.render(
            {
                "params": {
                    "schema_name": table["schema_name"],
                    "table_name": table["table_name"],
                }
            }
        )
        df = hook.get_polars_dataframe(query=query)
        return cast(int, df["cycleid"][0]) if len(df) else None

    def _upsert_current_cycleid(
        self,
        table: Table,
        cycleid: int,
    ):
        jinja_env = self.get_template_env()

        hook = self._get_airflow_upsert_hook()
        template = jinja_env.from_string(self.sql_upsert_last_synced_table_cycle_id)
        sql = template.render()
        hook.run(
            sql,
            parameters=[
                table["schema_name"],
                table["table_name"],
                cycleid,
            ],
        )
