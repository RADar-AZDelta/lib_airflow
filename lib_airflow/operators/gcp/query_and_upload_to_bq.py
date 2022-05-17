from typing import Any, Sequence

import backoff
import polars as pl
from lib_airflow.hooks.db.connectorx import ConnectorXHook
from lib_airflow.operators.gcp.upload_to_bq import UploadToBigQueryOperator


class QueryAndUploadToBigQueryOperator(UploadToBigQueryOperator):
    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        connectorx_db_conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.connectorx_db_conn_id = connectorx_db_conn_id

        self._db_hook = None

    def _query_to_parquet_and_upload(
        self,
        sql: str,
        object_name: str,
        table_metadata: Any = None,
    ) -> int:
        df = self._query(sql)
        return self._upload_parquet(df, object_name, table_metadata)

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _query(
        self,
        sql: str,
    ) -> pl.DataFrame:
        hook = self._get_db_hook()
        df = hook.get_polars_dataframe(query=sql)
        return df

    def _get_db_hook(self) -> ConnectorXHook:
        if not self._db_hook:
            self._db_hook = ConnectorXHook(
                connectorx_conn_id=self.connectorx_db_conn_id
            )
        return self._db_hook
