# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, Dict, Optional, Sequence, Union, cast

import backoff
import numpy as np
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context
from google.cloud import bigquery


class BigQueryToOtherGCSOperator(BaseOperator):
    """Does a query on the Bigquery of one project (of one organisation) and stores the result in a Cloud Storage bucket of another project (of another organisation)"""

    template_fields: Sequence[str] = (
        "bucket",
        "impersonation_chain",
        "bucket_dir",
        "start_page",
        "page_size",
        "project",
        "table",
        "dataset",
        "sql",
        "bucket",
        "filename",
        "impersonation_chain",
    )
    _bq_to_pandas_data_type_mapping = {
        "STRING": pd.StringDtype(),
        "BYTES": pd.StringDtype(),
        "INTEGER": pd.Int64Dtype(),
        "INT64": pd.Int64Dtype(),
        "NUMERIC": np.float64,
        "FLOAT64": np.float64,
        "FLOAT": np.float64,
        "BOOLEAN": pd.BooleanDtype(),
        "TIMESTAMP": pd.DatetimeTZDtype(tz="UTC"),
        "DATE": pd.DatetimeTZDtype(tz="UTC"),
        "DATETIME": pd.DatetimeTZDtype(tz="UTC"),
    }
    # _bq_to_arrow_data_type_mapping = {
    #     "STRING": pa.string(),
    #     "BYTES": pa.string(),
    #     "INTEGER": pa.int64(),
    #     "INT64": pa.int64(),
    #     "NUMERIC": pa.float64(),
    #     "FLOAT64": pa.float64(),
    #     "FLOAT": pa.float64(),
    #     "BOOLEAN": pa.bool_(),
    #     "TIMESTAMP": pa.timestamp(unit="ms"),
    #     "DATE": pa.date32(),
    #     "DATETIME": pa.string(),
    # }

    def __init__(
        self,
        *,
        bigquery_conn_id: str,
        cs_conn_id: str,
        project: str,
        dataset: str,
        table: str,
        sql: str = """{% raw %}
select *
from {{ project }}.{{ dataset }}.{{ table }}
LIMIT {{ page_size }} OFFSET {{ page * page_size }}
{% endraw %}""",
        start_page: Union[int, str] = 0,
        page_size: Union[int, str] = 100000,
        bucket_dir: str = "upload",
        func_page_loaded: Optional[Callable[[str, Context, int], None]] = None,
        bucket: str,
        filename: str = "{% raw %}upload_{{ project }}_{{ dataset }}_{{ table }}_page_{{ page }}.parquet{% endraw %}",
        gzip: bool = False,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            bigquery_conn_id (str): BigQuery connection ID
            cs_conn_id (str): Cloud Storage connection ID
            project (str): GCP project ID that holds the Bigquery
            dataset (str): BigQuery dataset ID
            table (str): BigQuery table name
            bucket (str): Name of the bucket
            sql (str, optional): The paged query.
            start_page (Union[int, str], optional): Page to start from. Defaults to 0.
            page_size (Union[int, str], optional): Number of rows to fetch in each page. Defaults to 100000.
            bucket_dir (str, optional): The bucket dir to store the paged upload Parquet files. Defaults to "upload".
            func_page_loaded (Optional[Callable[[str, Context, int], None]], optional): Function that can be called every time a page is uploaded. Defaults to None.
            filename (str, optional): The name of the uploaded Parquet file.
            gzip (bool, optional): Compress local file before upload (Parquet files are already zipped). Defaults to False.
            delegate_to (Optional[str], optional): This performs a task on one host with reference to other hosts. Defaults to None.
            impersonation_chain (Optional[Union[str, Sequence[str]]], optional): This is the optional service account to impersonate using short term credentials. Defaults to None.
        """
        super().__init__(**kwargs)

        self.bigquery_conn_id = bigquery_conn_id
        self.cs_conn_id = cs_conn_id
        self.project = project
        self.dataset = dataset
        self.table = table
        self.sql = sql
        self.start_page = start_page
        self.page_size = page_size
        self.bucket_dir = bucket_dir
        self.func_page_loaded = func_page_loaded
        self.bucket = bucket
        self.filename = filename
        self.gzip = gzip
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        self.file_mime_type = "application/octet-stream"
        self.gcs_hook = None
        self.bq_hook = None

    def execute(self, context: "Context") -> None:
        """Execute the operator.
        Do the paged SQL query and upload the results as Parquet to Cloud Storage.

        Args:
            context (Context):  Jinja2 template context for task rendering.
        """
        jinja_env = self.get_template_env()

        sql = """
select *
from {{ project }}.{{ dataset }}.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ table }}'
"""
        template = jinja_env.from_string(sql)
        sql = template.render(
            project=self.project,
            dataset=self.dataset,
            table=self.table,
        )
        bq_hook = self._get_bq_hook()
        df = bq_hook.get_pandas_df(sql, dialect="standard")
        dtypes = {}
        for index, row in df.iterrows():
            dtypes[row.column_name] = self._bq_to_pandas_data_type_mapping[
                row.data_type
            ]

        page = cast(int, self.start_page) or 0
        returned_rows = self.page_size
        while returned_rows == self.page_size:
            template = jinja_env.from_string(self.sql)
            sql = template.render(
                project=self.project,
                dataset=self.dataset,
                table=self.table,
                page_size=self.page_size,
                page=page,
            )
            template = jinja_env.from_string(self.filename)
            filename = template.render(
                project=self.project,
                dataset=self.dataset,
                table=self.table,
                page=page,
            )
            returned_rows = self._paged_upload(sql, filename, page, dtypes=dtypes)
            page += 1
            if self.func_page_loaded:
                self.func_page_loaded(self.table, context, page)

    def _paged_upload(
        self, sql: str, filename: str, page: int, dtypes: Dict[str, Any]
    ) -> int:
        """Do the paged SQL query and upload the results as Parquet to Cloud Storage.

        Args:
            sql (str): The paged SQL query
            filename (str): The bucket filename to upload the parquet file to
            page (int): The current page to execute and upload
            dtypes (Dict[str, Any]): A dictionary of column names pandas ``dtype``s. The provided ``dtype`` is used when constructing the series for the column specified. Otherwise, the default pandas behavior is used.

        Returns:
            int: _description_
        """
        df = self._query(sql, dtypes)
        returned_rows = len(df)
        self.log.info(f"Rows fetched for page {page}: {returned_rows}")

        if returned_rows > 0:
            with self._write_local_data_files(df) as file_to_upload:
                file_to_upload.flush()
                self._upload_to_gcs(
                    file_to_upload,
                    f"{self.bucket_dir}/{filename}",
                )
        return returned_rows

    def _write_local_data_files(self, df: pd.DataFrame) -> _TemporaryFileWrapper:
        """Writes the pandas dataframe to a temporary Parquet file

        Args:
            df (pd.DataFrame): The pandas dataframe

        Returns:
            _TemporaryFileWrapper: The temporary file
        """
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)

        df.to_parquet(tmp_file_handle.name, engine="pyarrow")
        return tmp_file_handle

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _query(self, sql, dtypes: Dict[str, Any]) -> pd.DataFrame:
        """Do a query into a pandas dataframe

        Args:
            sql (str): The SQL query
            dtypes (Dict[str, Any]): A dictionary of column names pandas ``dtype``s. The provided ``dtype`` is used when constructing the series for the column specified. Otherwise, the default pandas behavior is used.

        Returns:
            pd.DataFrame: The pandas DataFrame holding the query results
        """
        self.log.info("Running query '%s", sql)

        bq_hook = BigQueryHook(gcp_conn_id="gcp_awell")

        bq_client = bigquery.Client(
            project=bq_hook._get_field("project"),
            credentials=bq_hook._get_credentials(),
        )
        df = bq_client.query(sql).to_dataframe(dtypes=dtypes, date_as_object=False)
        return df

    @backoff.on_exception(
        backoff.expo,
        (Exception),
        max_time=600,
        max_tries=20,
    )
    def _upload_to_gcs(self, file_to_upload, filename):
        """Upload file to Cloud Storage

        Args:
            file_to_upload (str): Name that the uploaded file will have in the bucket
            filename (str): File to upload
        """
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = self._get_gcs_hook()
        hook.upload(
            self.bucket,
            filename,
            file_to_upload.name,
            mime_type=self.file_mime_type,
            gzip=self.gzip,
        )

    def _get_gcs_hook(self) -> GCSHook:
        """Get the Cloud Storage Hook

        Returns:
            GCSHook: The Cloud Storage Hook
        """
        if not self.gcs_hook:
            self.gcs_hook = GCSHook(
                gcp_conn_id=self.cs_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        return self.gcs_hook

    def _get_bq_hook(self) -> BigQueryHook:
        """Get the BigQuery Hook

        Returns:
            BigQueryHook: The BigQuery Hook
        """
        if not self.bq_hook:
            self.bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id)
        return self.bq_hook
