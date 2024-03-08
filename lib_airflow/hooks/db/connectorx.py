# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+"""ConnectorX to GCS operator."""

from typing import Any, List, Optional, Tuple, Union, cast
from urllib.parse import unquote

import connectorx as cx
import pandas as pd
import polars as pl
import pyarrow as pa
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


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
        """Constructor"""
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
        return_type: str = "polars",
        partition_on: Optional[str] = None,
        partition_range: Optional[Tuple[int, int]] = None,
        partition_num: Optional[int] = None,
    ) -> pa.Table | pl.DataFrame | pd.DataFrame:
        """Run the hook. Execute the query with ConnectorX.

        Args:
            query (Optional[Union[List[str], str]], optional): The SQL query. Defaults to None.
            protocol (str, optional): Backend-specific transfer protocol directive. Defaults to "binary".
            return_type (str, optional): Return type of this function; one of "arrow2", "pandas" or "polars". Defaults to "arrow2".
            partition_on (Optional[str], optional): The column on which to partition the result.. Defaults to None.
            partition_range (Optional[Tuple[int, int]], optional): The value range of the partition column. Defaults to None.
            partition_num (Optional[int], optional): How many partitions to generate.. Defaults to None.

        Returns:
            Any: Arrow table, Polars dataframe or Pandas dataframe
        """
        if self.connection.conn_type == "google_cloud_platform":
            conn = f"bigquery://{self.connection.extra_dejson['extra__google_cloud_platform__key_path']}"
        else:
            conn = unquote(self.connection.get_uri())

        df = cx.read_sql(
            conn=conn,
            query=query,  # type: ignore
            return_type=return_type,
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
            df: pl.DataFrame = cast(pl.DataFrame, self.run("select 1"))
            if len(df) == 1:
                status = True
                message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message
