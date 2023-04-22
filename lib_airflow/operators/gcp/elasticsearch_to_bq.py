import json
import re
import traceback
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable, List, Sequence

import polars as pl
import pyarrow as pa
from airflow.models import Variable
from airflow.utils.email import send_email
from elasticsearch import Elasticsearch

from . import UploadToBigQueryOperator


class ElasticSearchToBigQueryOperator(UploadToBigQueryOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "bucket_dir",
    )

    def __init__(
        self,
        elastic_url: str,
        api_key_var_id: str,
        destination_dataset: str,
        indexes: List[str],
        es_batch_size=1000,
        pq_page_size=100000,
        bucket_dir: str = "upload",
        func_modify_doc: Callable[[Any], Any] | None = None,
        to_email_on_error: list[str] | Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.elastic_url = elastic_url
        self.api_key_var_id = api_key_var_id
        self.indexes = indexes
        self.es_batch_size = es_batch_size
        self.pq_page_size = pq_page_size
        self.bucket_dir = bucket_dir
        self.destination_dataset = destination_dataset
        self.func_modify_doc = func_modify_doc
        self.to_email_on_error = to_email_on_error

    def execute(self, context):
        api_key = Variable.get(self.api_key_var_id)
        client = Elasticsearch(self.elastic_url, api_key=api_key)

        for num, index in enumerate(self.indexes):
            self.log.info(f"ES index: {index}")
            str_error = None
            try:
                self._full_upload_es_table(client, index)
            except Exception as ex:
                str_error = traceback.format_exc()
                self.log.error("Error upploading ES index '%s': %s", index, ex)

            if str_error:
                try:
                    if self.to_email_on_error:
                        send_email(
                            to=self.to_email_on_error,
                            subject=f"AIRFLOW ERROR in dag '{self.dag_id}' for ES index '{index}'",
                            html_content=str_error.replace("\n", "<br />"),
                        )
                except:
                    pass

    def _full_upload_es_table(self, client: Elasticsearch, index: str):
        # keep track of the number of the documents returned
        doc_count = 0
        page = 0

        # declare a filter query dict object
        match_all = {"size": self.es_batch_size, "query": {"match_all": {}}}

        # make a search() request to get all docs in the index
        resp = client.search(
            index=index,
            body=match_all,
            scroll="20s",  # length of time to keep search context
        )

        self.log.info(
            f'ES index "{index}" has {resp["hits"]["total"]["value"]} documents'
        )

        table = re.sub(r"[^a-zA-Z_\-]", "", index)

        docs = []

        for doc in resp["hits"]["hits"]:
            self.log.debug(f'ID: {doc["_id"]}')
            source = doc["_source"]
            self.log.debug(f"DOC: {json.dumps(source)}")
            if self.func_modify_doc:
                source = self.func_modify_doc(source)
            docs.append(source)
            doc_count += 1
        self.log.debug(f"DOC COUNT: {doc_count}")

        #  keep track of pass scroll _id
        old_scroll_id = resp["_scroll_id"]

        # use a 'while' iterator to loop over document 'hits'
        while len(resp["hits"]["hits"]):
            # make a request using the Scroll API
            resp = client.scroll(
                scroll_id=old_scroll_id,
                scroll="20s",  # length of time to keep search context
            )

            # check if there's a new scroll ID
            if old_scroll_id != resp["_scroll_id"]:
                self.log.debug(f'NEW SCROLL ID: {resp["_scroll_id"]}')

            # keep track of pass scroll _id
            old_scroll_id = resp["_scroll_id"]

            # iterate over the document hits for each 'scroll'
            for doc in resp["hits"]["hits"]:
                self.log.debug(f'ID: {doc["_id"]}')
                source = doc["_source"]
                self.log.debug(f"DOC: {json.dumps(source)}")
                if self.func_modify_doc:
                    source = self.func_modify_doc(source)
                docs.append(source)
                doc_count += 1
            self.log.debug(f"DOC COUNT: {doc_count}")

            if doc_count >= self.pq_page_size:
                # table = pa.Table.from_pylist(docs)
                # df = pl.from_arrow(table)
                df = pl.from_dicts(docs)
                self._upload_parquet(
                    df,
                    object_name=f"{self.bucket_dir}/{table}/full/{table}_{page}.parquet",
                    table_metadata=object(),
                )
                docs = []
                doc_count = 0
                page += 1

        if doc_count == 0 and page == 0:
            self.log.warning(f"Nothing to FULL upload for index {index}")
        elif doc_count > 0:
            # table = pa.Table.from_pylist(docs)
            # df = pl.from_arrow(table)
            df = pl.from_dicts(docs)
            self._upload_parquet(
                df,
                object_name=f"{self.bucket_dir}/{table}/full/{table}_{page}.parquet",
                table_metadata=object(),
            )

        if self._check_parquet(
            f"{self.bucket_dir}/{table}/full/{table}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table}/full/{table}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_dataset}.{table}",
                # cluster_fields=
            )
