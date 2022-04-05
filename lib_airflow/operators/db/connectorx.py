"""ConnectorX to GCS operator."""

from typing import Callable, Optional, Sequence

import polars as pl
from airflow.models.baseoperator import BaseOperator
from lib_airflow.hooks.db.connectorx import ConnectorXHook


class ConnectorXOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql", ".sql.jinja")
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#e0a98c"
    _hook = None

    def __init__(
        self,
        *,
        connectorx_conn_id: str,
        sql: str,
        func_modify_data: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.sql = sql
        self.func_modify_data = func_modify_data

    def get_hook(self):
        if not self._hook:
            self._hook = ConnectorXHook(connectorx_conn_id=self.connectorx_conn_id)
        return self._hook

    def execute(self, context: "Context"):
        self.log.info("Executing SQL: %s", self.sql)
        hook = self.get_hook()

        df = hook.get_polars_dataframe(query=self.sql)

        if self.func_modify_data:
            df = self.func_modify_data(df)

        return df.write_json(row_oriented=True)
