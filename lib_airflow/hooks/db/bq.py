# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+"""ConnectorX to GCS operator."""

from typing import Any, List, Optional, Tuple, Union
from urllib.parse import unquote

import google.auth
import google.cloud.bigquery as bq
import polars as pl
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class BigQueryHook(BaseHook):
    conn_name_attr = "conn_id"
    default_conn_name = "default"
    _connection = None
    _client = None

    def __init__(
        self,
        location: str = "EU",
        *args,
        **kwargs,
    ) -> None:
        """Constructor"""
        super().__init__()

        self._location = location

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

    @property
    def client(self):
        if not self._client:
            credentials, _ = google.auth.load_credentials_from_file(
                filename=self.connection.extra_dejson[
                    "extra__google_cloud_platform__key_path"
                ]
            )
            self._client = bq.Client(credentials=credentials)
        return self._client

    def run(
        self,
        query: str,
    ) -> Any:
        job_config = bq.QueryJobConfig(
            query_parameters=[],
        )
        query_job = self.client.query(
            query, job_config=job_config, location=self._location
        )
        result = query_job.result()
        return pl.from_arrow(result.to_arrow())
