"""ConnectorX to GCS operator."""

from typing import Sequence

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.sql = sql

    def get_hook(self):
        if not self._hook:
            self._hook = ConnectorXHook(connectorx_conn_id=self.connectorx_conn_id)
        return self._hook

    def execute(self, context: "Context"):
        self.log.info("Executing SQL: %s", self.sql)
        hook = self.get_hook()

        df = hook.get_polars_dataframe(query=self.sql)

        return df.write_json(row_oriented=True)
