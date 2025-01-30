import json
import tempfile
import time
import traceback
import uuid
from http import HTTPStatus
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import jinja2
import polars as pl
import pyarrow as pa
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context
from airflow.utils.email import send_email
from pyarrow import json as pajson

from libs.lib_airflow.lib_airflow.utils.airflow_encoder import UtcJsonEncoder

from .upload_to_bq import UploadToBigQueryOperator


class PagedRestToBigQueryOperator(UploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
        "endpoints",
    )

    def __init__(
        self,
        http_conn_id: str,
        endpoints: dict[str, Tuple[str, Any]] | str,
        destination_dataset: str,
        func_create_merge_statement: Callable[
            [str, str, str, Any, pl.DataFrame, jinja2.Environment], str
        ],
        func_modify_dataframe: Callable[
            [str, str, str, Any, pl.DataFrame], pl.DataFrame
        ]
        | None = None,
        func_get_auth_token: Callable[[], str] | None = None,
        func_get_request_data: Callable[
            [str, str, Any, int, int, list[Any]], Optional[Union[Dict[str, Any], str]]
        ]
        | None = None,
        func_extract_records_from_response_data: Callable[
            [str, str, Any, Any, Optional[Union[Dict[str, Any], str]]], list[Any]
        ]
        | None = None,
        func_get_schema: Callable[[str, str, Any, Any], pa.schema] | None = None,
        func_get_parquet_upload_path: Callable[[str, str, Any, pl.DataFrame], str]
        | None = None,
        func_get_cluster_fields: Callable[[str, str, Any], list[str]] | None = None,
        func_batch_uploaded: Callable[
            [str, str, Any, pl.DataFrame, jinja2.Environment, Any], None
        ]
        | None = None,
        func_fetch_more_data: Callable[
            [
                str,
                str,
                Any,
                int,
                int,
                Optional[Union[Dict[str, Any], str]],
                Optional[list[Any]],
            ],
            bool,
        ]
        | None = None,
        page_size: int = 1000,
        upload_after_nbr_of_pages=100,
        bucket_dir: str = "upload",
        http_method="GET",
        authorization_header_name="Authorization",
        json_decoder=json.JSONDecoder,
        to_email_on_error: list[str] | Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.http_conn_id = http_conn_id
        self.http_method = http_method
        self.func_get_auth_token = func_get_auth_token
        self.json_decoder = json_decoder
        self.authorization_header_name = authorization_header_name

        self.endpoints = endpoints

        self.func_modify_dataframe = func_modify_dataframe
        self.func_create_merge_statement = func_create_merge_statement
        self.func_get_request_data = func_get_request_data
        self.func_extract_records_from_response_data = (
            func_extract_records_from_response_data
        )
        self.func_get_schema = func_get_schema
        self.func_get_parquet_upload_path = func_get_parquet_upload_path
        self.func_get_cluster_fields = func_get_cluster_fields
        self.func_batch_uploaded = func_batch_uploaded
        self.func_fetch_more_data = (
            func_fetch_more_data if func_fetch_more_data else self._fetch_more_data
        )

        self.page_size = page_size
        self.upload_every_nbr_of_pages = upload_after_nbr_of_pages
        self.bucket_dir = bucket_dir
        self.destination_dataset = destination_dataset
        self.to_email_on_error = to_email_on_error

    def execute(self, context: "Context"):
        http = HttpHook(self.http_method, self.http_conn_id)

        for name, (url, endpoint_data) in cast(
            dict[str, Tuple[str, Any]], self.endpoints
        ).items():
            self.log.info(f"Endpoint: {name}")
            str_error = None
            try:
                self._upload_rest_endpoint(http, name, url, endpoint_data)
            except Exception as ex:
                str_error = traceback.format_exc()
                self.log.error("Error on endpoint '%s': %s", name, ex)
                breakpoint()

            if str_error:
                try:
                    if self.to_email_on_error:
                        send_email(
                            to=self.to_email_on_error,
                            subject=f"AIRFLOW ERROR in dag '{self.dag_id}' for REST endpoint '{name}'",
                            html_content=str_error.replace("\n", "<br />"),
                        )
                except:
                    pass

    def _fetch_more_data(
        self,
        endpoint_name: str,
        endpoint_url: str,
        endpoint_data: Any,
        request_page: int,
        page_size: int,
        request_data: Optional[Union[Dict[str, Any], str]],
        current_page_data: Optional[list[Any]],
    ):
        if request_page == 0:
            return True
        return current_page_data and len(current_page_data) == self.page_size

    def _upload_rest_endpoint(
        self, http: HttpHook, endpoint_name: str, endpoint_url: str, endpoint_data: Any
    ):
        request_page = 0
        all_data = []
        upload_batch = 0
        upload_batch_page = 0
        current_page_data: list[Any] = []
        response_data: Any = None
        request_data: Optional[Union[Dict[str, Any], str]] = None

        auth_token = None
        if self.func_get_auth_token:
            auth_token = self.func_get_auth_token()

        while self.func_fetch_more_data(
            endpoint_name,
            endpoint_url,
            endpoint_data,
            request_page,
            self.page_size,
            request_data,
            current_page_data,
        ):
            request_data = None
            if self.func_get_request_data:
                request_data = self.func_get_request_data(
                    endpoint_name,
                    endpoint_url,
                    endpoint_data,
                    request_page,
                    self.page_size,
                    current_page_data,
                )

            headers = {}
            if auth_token:
                headers[self.authorization_header_name] = auth_token

            try:
                response = http.run(
                    endpoint=endpoint_url,
                    data=request_data,
                    headers=headers,
                    # extra_options={"check_response": False},
                )
            except AirflowException as ex:
                self.log.warning("%s", ex)
                if (
                    ex.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
                    and ex.args[0] == "401:Unauthorized"
                    and self.func_get_auth_token
                ):
                    time.sleep(5)  # wait 5 sec
                    auth_token = self.func_get_auth_token()
                    continue
                else:
                    if all_data:
                        self._convert_to_parquet_and_upload(
                            all_data,
                            endpoint_name,
                            endpoint_url,
                            endpoint_data,
                            response_data,
                        )
                    raise ex

            response_data = json.loads(response.text, cls=self.json_decoder)
            current_page_data = (
                self.func_extract_records_from_response_data(
                    endpoint_name,
                    endpoint_url,
                    endpoint_data,
                    response_data,
                    request_data,
                )
                if self.func_extract_records_from_response_data
                else response_data
            )

            if current_page_data:
                all_data.extend(current_page_data)

            if all_data and upload_batch_page >= (self.upload_every_nbr_of_pages - 1):
                self._convert_to_parquet_and_upload(
                    all_data, endpoint_name, endpoint_url, endpoint_data, response_data
                )
                all_data = []
                upload_batch += 1
                upload_batch_page = 0
            else:
                upload_batch_page += 1

            request_page += 1

        if all_data:
            self._convert_to_parquet_and_upload(
                all_data, endpoint_name, endpoint_url, endpoint_data, response_data
            )

    def _convert_to_parquet_and_upload(
        self,
        all_data: list[Any],
        endpoint_name: str,
        endpoint_url: str,
        endpoint_data: Any,
        last_response_data: Any,
    ):
        schema = None
        if self.func_get_schema:
            schema = self.func_get_schema(
                endpoint_name, endpoint_url, endpoint_data, all_data
            )

        # # code to test if the data is invalid to the schema
        # for item in all_data:
        #     try:
        #         json_object = json.dumps(item, indent=4, cls=UtcJsonEncoder)
        #         with tempfile.NamedTemporaryFile() as tmp:
        #             with open(tmp.name, "w") as f:
        #                 f.write(json_object)
        #             t = pajson.read_json(
        #                 tmp.name,
        #                 parse_options=pajson.ParseOptions(
        #                     explicit_schema=arrow_schema,
        #                     unexpected_field_behavior="ignore",
        #                 ),
        #             )
        #             print(t.to_pydict())
        #             pl.from_arrow(t)
        #     except Exception as ex:
        #         breakpoint()
        # for item in all_data:
        #     try:
        #         table = pa.Table.from_pylist([item], schema=arrow_schema)
        #         df = pl.from_arrow(table)
        #     except Exception as ex:
        #         breakpoint()
        try:
            df = pl.from_dicts(all_data, schema=schema, strict=True)
        except Exception as ex:
            breakpoint()
            raise ex

        # table = pa.Table.from_pylist(all_data, schema=schema)
        # df = pl.from_arrow(table)  # throws ('a StructArray must contain at least one field',)
        # df = pl.from_pandas(table.to_pandas())
        df = self._check_dataframe_for_bigquery_safe_column_names(df)
        endpoint_name = self._generate_bigquery_safe_table_name(
            endpoint_name
        )  # not yet implemented
        if self.func_modify_dataframe:
            df = self.func_modify_dataframe(
                self.destination_dataset,
                endpoint_name,
                endpoint_url,
                endpoint_data,
                df,
            )

        if self.func_get_parquet_upload_path:
            upload_path = self.func_get_parquet_upload_path(
                endpoint_name, endpoint_url, endpoint_data, df
            )
            object_name: str = f"{self.bucket_dir}/{upload_path}"
        else:
            object_name: str = f"{self.bucket_dir}/{endpoint_name}.parquet"

        self._upload_parquet(df, object_name=object_name, table_metadata=endpoint_data)

        cluster_fields = None
        if self.func_get_cluster_fields:
            cluster_fields = self.func_get_cluster_fields(
                endpoint_name, endpoint_url, endpoint_data
            )

        self._load_parquets_in_bq(
            source_uris=[f"gs://{self.bucket}/{object_name}"],
            destination_project_dataset_table=f"{self.destination_dataset}._incremental_{endpoint_name}",
            cluster_fields=cluster_fields,
        )

        sql = self.func_create_merge_statement(
            self.destination_dataset,
            endpoint_name,
            endpoint_url,
            endpoint_data,
            df,
            self.get_template_env(),
        )
        self._run_bq_job(
            sql,
            job_id=f"airflow_{self.destination_dataset}_{endpoint_name}_{str(uuid.uuid4())}",
        )
        self._delete_bq_table(
            dataset_table=f"{self.destination_dataset}._incremental_{endpoint_name}",
        )

        if self.func_batch_uploaded:
            self.func_batch_uploaded(
                endpoint_name,
                endpoint_url,
                endpoint_data,
                df,
                self.get_template_env(),
                last_response_data,
            )
