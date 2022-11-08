# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import re
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, List, Optional, Sequence, Union

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob
from google.cloud.bigquery.retry import DEFAULT_RETRY as BQ_DEFAULT_RETRY

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class UploadToBigQueryOperator(BaseOperator):
    """Uploads Polars DataFrames to Gogle Cloud Storage"""

    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        bucket: str,
        gcp_cs_conn_id: str,
        gcp_bq_conn_id: str,
        func_modify_data: Optional[
            Callable[[pl.DataFrame | pa.Table, Optional[Any]], pl.DataFrame | pa.Table]
        ] = None,
        **kwargs,
    ) -> None:
        """Constructor

        Args:
            bucket (str): The Gcloud Storage Bucket
            gcp_cs_conn_id (str): Cloud Storage connection ID
            gcp_bq_conn_id (str): BigQuery connection ID
            func_modify_data (Optional[ Callable[[pl.DataFrame, Optional[Any]], pl.DataFrame] ], optional): Function to modify the data in the DataFrame
        """
        super().__init__(**kwargs)
        self.func_modify_data = func_modify_data
        self.bucket = bucket
        self.gcp_cs_conn_id = gcp_cs_conn_id
        self.gcp_bq_conn_id = gcp_bq_conn_id

        self._gcs_hook = None
        self._bq_hook = None

    def _rename_bigquery_column_names(self, columns: List[str]) -> List[str]:
        """Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 300 characters long

        Args:
            columns (List[str]): The list of columns

        Returns:
            List[str]: The list of columns
        """
        for i, column in enumerate(columns):
            if "-" in column:
                columns[i] = column.replace("-", "_")
            if column[0].isdigit():
                columns[i] = f"_{column}"

        return columns

    def _delete_bq_table(self, dataset_table: str) -> None:
        """Delete a BigQuery table

        Args:
            dataset_table (str): The identifier of the job
        """
        hook = self._get_bq_hook()
        hook.delete_table(table_id=dataset_table)

    def _run_bq_job(self, sql: str, job_id: str) -> BigQueryJob:
        """Run a BigQuery job

        Args:
            sql (str): The query
            job_id (str): The identifier of the job

        Returns:
            BigQueryJob: The running BigQuery job
        """
        hook = self._get_bq_hook()
        configuration = {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        }
        job_id = re.sub(r"[:\-+.]", "_", job_id)
        return hook.insert_job(
            configuration=configuration, job_id=job_id, retry=BQ_DEFAULT_RETRY
        )

    def _load_parquets_in_bq(
        self,
        source_uris: List[str],
        destination_project_dataset_table: str,
        cluster_fields: Optional[List[str]] = None,
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=None,
    ) -> None:
        """Loads the parquet file(s) stored in Cloud Storage into BigQuery.

        Args:
            source_uris (List[str]):  The source Google Cloud Storage URI(s)
            destination_project_dataset_table (str): The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery table to load data into.
            cluster_fields (Optional[List[str]], optional): Request that the result of this load be stored sorted by one or more columns. Defaults to None.
            write_disposition (str, optional): The write disposition if the table already exists. Defaults to "WRITE_TRUNCATE".
            schema_update_options (_type_, optional): Allows the schema of the destination table to be updated as a side effect of the load job. Defaults to None.
        """
        bq_hook = self._get_bq_hook()

        bq_hook.run_load(
            destination_project_dataset_table=destination_project_dataset_table,
            source_uris=source_uris,
            source_format="PARQUET",
            write_disposition=write_disposition,
            cluster_fields=cluster_fields,
            autodetect=True,
            schema_update_options=schema_update_options,
        )

    def _check_parquet(self, bucket_object_prefix: str) -> bool:
        """Check if the Parquet file with the specific prefix already exists in the bucket

        Args:
            bucket_object_prefix (str): Bucket object prefix name

        Returns:
            bool: True if it already exists, otherwise false
        """
        hook = self._get_gcs_hook()
        try:
            count = len(hook.list(self.bucket, prefix=bucket_object_prefix))
            return count > 0
        except:
            return False

    def _upload_parquet(
        self,
        df: pl.DataFrame | pa.Table,
        object_name: str,
        table_metadata: Any = None,
    ) -> int:
        """Upload DataFrame by converting it ot Parquet in a temporary file, and uploading it to Cloud Storage.

        Args:
            df (pl.DataFrame): Polars DataFrame
            object_name (str): _description_
            table_metadata (Any, optional): Metadata object, that if supplied is passed to func_modify_data method. This can be used to pseudonomise data for instance.  Defaults to None.

        Returns:
            int: Number of uploaded rows
        """
        returned_rows = len(df)
        self.log.debug(f"Rows fetched: {returned_rows}")

        if returned_rows > 0:
            if self.func_modify_data:
                df = self.func_modify_data(df, table_metadata)
            with self._write_local_data_files(df) as file_to_upload:
                file_to_upload.flush()
                self._upload_to_gcs(file_to_upload, object_name)
        return returned_rows

    def _write_local_data_files(
        self, df: pl.DataFrame | pa.Table
    ) -> _TemporaryFileWrapper:
        """Write the DataFrame to a temporary Parquet file

        Args:
            df (pl.DataFrame): Our DataFrame

        Returns:
            _TemporaryFileWrapper: The temporary Parquet file
        """
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)

        if isinstance(df, pl.DataFrame):
            # df = df.to_arrow() # convert to an Arrow Table
            df.write_parquet(file=tmp_file_handle.name, compression="snappy")
        else:
            pq.write_table(
                table=df,
                where=tmp_file_handle.name,
                compression="snappy",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )
        return tmp_file_handle

    def _upload_to_gcs(
        self, file_to_upload: _TemporaryFileWrapper, object_name: str
    ) -> None:
        """Upload the Parquet temporary file to Cloud Storage

        Args:
            file_to_upload (_TemporaryFileWrapper): Parquet file to upload
            object_name (str): The file name to set in the bucket
        """
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = self._get_gcs_hook()
        hook.upload(
            bucket_name=self.bucket,
            object_name=object_name,
            filename=file_to_upload.name,
        )

    def _get_gcs_hook(self) -> GCSHook:
        """Get the Cloud Storage Hook

        Returns:
            GCSHook: The Cloud Storage Hook
        """
        if not self._gcs_hook:
            self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_cs_conn_id)
        return self._gcs_hook

    def _get_bq_hook(self) -> BigQueryHook:
        """Get the BigQuery Hook

        Returns:
            BigQueryHook: The BigQuery Hook
        """
        if not self._bq_hook:
            self._bq_hook = BigQueryHook(
                gcp_conn_id=self.gcp_bq_conn_id,
            )
        return self._bq_hook
