import os
from typing import Any, List, Optional, Tuple, Union
from urllib.parse import quote_plus, unquote

import jinja2 as jj
import pandas as pd
import polars as pl
import pyarrow as pa
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from jinja2.utils import select_autoescape

import connectorx as cx


class ConnectorXHook(BaseHook):
    """
    Interact with db data sources using ConnectorX.

    See [ConnectorX](https://github.com/sfu-db/connector-x) for full documentation.
    """

    conn_name_attr = "connectorx_conn_id"
    default_conn_name = "connectorx_default"
    _connection = None

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()

        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

    @property
    def connection(self):
        """``airflow.Connection`` object with connection id ``odbc_conn_id``"""
        if not self._connection:
            self._connection = self.get_connection(getattr(self, self.conn_name_attr))
        return self._connection

    def run(
        self,
        query: Optional[Union[List[str], str]] = None,
        protocol: str = "binary",
        return_type: str = "arrow2",
        partition_on: Optional[str] = None,
        partition_range: Optional[Tuple[int, int]] = None,
        partition_num: Optional[int] = None,
    ) -> Any:
        return cx.read_sql(
            conn=unquote(self.connection.get_uri()),
            query=query,  # type: ignore
            return_type=return_type,
            protocol=protocol,
            partition_on=partition_on,
            partition_range=partition_range,
            partition_num=partition_num,
        )

    def get_arrow_table(
        self,
        query: Optional[Union[List[str], str]] = None,
        protocol: str = "binary",
        partition_on: Optional[str] = None,
        partition_range: Optional[Tuple[int, int]] = None,
        partition_num: Optional[int] = None,
    ) -> pa.Table:
        table: pa.Table = self.run(
            query=query,  # type: ignore
            return_type="arrow",
            protocol=protocol,
            partition_on=partition_on,
            partition_range=partition_range,
            partition_num=partition_num,
        )
        return table

    def get_polars_dataframe(
        self,
        query: Optional[Union[List[str], str]] = None,
        protocol: str = "binary",
        partition_on: Optional[str] = None,
        partition_range: Optional[Tuple[int, int]] = None,
        partition_num: Optional[int] = None,
    ) -> pl.DataFrame:
        df: pl.DataFrame = self.run(
            query=query,  # type: ignore
            return_type="polars",
            protocol=protocol,
            partition_on=partition_on,
            partition_range=partition_range,
            partition_num=partition_num,
        )
        return df

    def test_connection(self):
        """Tests the connection by executing a select 1 query"""
        status, message = False, ""
        try:
            table = self.get_arrow_table("select 1")
            if table.num_rows == 1:
                status = True
                message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message
