# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+"""ConnectorX to GCS operator."""

from typing import Callable, Optional, Sequence, cast

import polars as pl
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from ...hooks.db.connectorx import ConnectorXHook


class ConnectorXOperator(BaseOperator):
    """Operator that executes a SQL query with ConnectorX and returns the results as JSON."""

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
        func_modify_data: Optional[
            Callable[[pl.DataFrame, Context], pl.DataFrame]
        ] = None,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            connectorx_conn_id (str): Connection ID
            sql (str): The SQL query
            func_modify_data (Optional[ Callable[[pl.DataFrame, Context], pl.DataFrame] ], optional): This function lets you modify the results of the Query. With this function you can do for instance do pseudonimisation. Defaults to None.
        """
        super().__init__(**kwargs)

        self.connectorx_conn_id = connectorx_conn_id
        self.sql = sql
        self.func_modify_data = func_modify_data

    def get_hook(self) -> ConnectorXHook:
        """Get the ConnectorX hook

        Returns:
            ConnectorXHook: The ConnectorX hook
        """
        if not self._hook:
            self._hook = ConnectorXHook(connectorx_conn_id=self.connectorx_conn_id)
        return self._hook

    def execute(self, context: "Context") -> str:
        """Executes the operator.
        Does the query with ConnectorX and converts the results to a JSON string.

        Args:
            context (Context):  Jinja2 template context for task rendering.

        Returns:
            str: The query result as JSON
        """
        self.log.info("Executing SQL: %s", self.sql)
        hook = self.get_hook()

        df = cast(pl.DataFrame, hook.run(self.sql))

        if self.func_modify_data:
            df = self.func_modify_data(df, context)

        return df.write_json(row_oriented=True)
