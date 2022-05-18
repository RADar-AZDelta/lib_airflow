import re
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, List, Optional, Sequence, Tuple, Union, cast

import polars as pl
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.bigquery.retry import DEFAULT_RETRY as BQ_DEFAULT_RETRY


class UploadToBigQueryOperator(BaseOperator):
    template_fields: Sequence[str] = ("bucket",)

    def __init__(
        self,
        bucket: str,
        gcp_cs_conn_id: str,
        gcp_bq_conn_id: str,
        func_modify_data: Optional[
            Callable[[pl.DataFrame, Optional[Any]], pl.DataFrame]
        ] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.func_modify_data = func_modify_data
        self.bucket = bucket
        self.gcp_cs_conn_id = gcp_cs_conn_id
        self.gcp_bq_conn_id = gcp_bq_conn_id

        self._gcs_hook = None
        self._bq_hook = None

    def _rename_bigquery_column_names(self, columns: List[str]) -> List[str]:
        # Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 300 characters long
        for i, column in enumerate(columns):
            if "-" in column:
                columns[i] = column.replace("-", "_")
            if column[0].isdigit():
                columns[i] = f"_{column}"

        return columns

    def _run_bq_job(self, sql, job_id: str):
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
    ):
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

    def _check_parquet(self, bucket_object: str):
        hook = self._get_gcs_hook()
        try:
            count = len(hook.list(self.bucket, prefix=bucket_object))
            return count > 0
        except:
            return False

    def _upload_parquet(
        self,
        df: pl.DataFrame,
        object_name: str,
        table_metadata: Any = None,
    ) -> int:
        returned_rows = len(df)
        self.log.debug(f"Rows fetched: {returned_rows}")

        if returned_rows > 0:
            if self.func_modify_data:
                df = self.func_modify_data(df, table_metadata)
            with self._write_local_data_files(df) as file_to_upload:
                file_to_upload.flush()
                self._upload_to_gcs(file_to_upload, object_name)
        return returned_rows

    def _write_local_data_files(self, df: pl.DataFrame) -> _TemporaryFileWrapper:
        tmp_file_handle = NamedTemporaryFile(delete=True)
        self.log.info("Writing parquet files to: '%s'", tmp_file_handle.name)
        table = df.to_arrow()

        pq.write_table(
            table,
            tmp_file_handle.name,
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
        )
        return tmp_file_handle

    def _upload_to_gcs(self, file_to_upload: _TemporaryFileWrapper, object_name: str):
        self.log.info("Uploading '%s' to GCS.", file_to_upload.name)
        hook = self._get_gcs_hook()
        hook.upload(
            bucket_name=self.bucket,
            object_name=object_name,
            filename=file_to_upload.name,
        )

    def _get_gcs_hook(self):
        if not self._gcs_hook:
            self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_cs_conn_id)
        return self._gcs_hook

    def _get_bq_hook(self):
        if not self._bq_hook:
            self._bq_hook = BigQueryHook(
                gcp_conn_id=self.gcp_bq_conn_id,
            )
        return self._bq_hook
