import json
import re
from tempfile import NamedTemporaryFile
from typing import List, Sequence

import polars as pl
from airflow.models import Variable
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
        destination_project_dataset: str,
        indexes: List[str],
        es_batch_size=1000,
        pq_page_size=100000,
        bucket_dir: str = "upload",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.elastic_url = elastic_url
        self.api_key_var_id = api_key_var_id
        self.indexes = indexes
        self.es_batch_size = es_batch_size
        self.pq_page_size = pq_page_size
        self.bucket_dir = bucket_dir
        self.destination_project_dataset = destination_project_dataset

    def execute(self, context):
        api_key = Variable.get("awell_api_key")
        client = Elasticsearch(self.elastic_url, api_key=api_key)

        for num, index in enumerate(self.indexes):
            self.log.info(f"ES index: {index}")
            self._full_upload_es_table(client, index)

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

        table_name = re.sub(r"[^a-zA-Z_\-]", "", index)

        tmp_file_handle = NamedTemporaryFile(delete=True)
        json_file = open(tmp_file_handle.name, "w")
        json_file.write("[")

        for doc in resp["hits"]["hits"]:
            self.log.debug(f'ID: {doc["_id"]}')
            self.log.debug(f'DOC: {json.dumps(doc["_source"])}')
            if doc_count > 0:
                json_file.write(",\n")
            json_file.write(json.dumps(doc["_source"], indent=2))
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
                self.log.debug(f'DOC: {json.dumps(doc["_source"])}')
                if doc_count > 0:
                    json_file.write(",\n")
                json_file.write(json.dumps(doc["_source"], indent=2))
                doc_count += 1
                self.log.debug(f"DOC COUNT: {doc_count}")

            if doc_count > self.pq_page_size:
                json_file.write("]")
                json_file.close()
                df = pl.read_json(file=tmp_file_handle.name, json_lines=False)
                self._upload_parquet(
                    df,
                    object_name=f"{self.bucket_dir}/{table_name}/full/{table_name}_{page}.parquet",
                )
                tmp_file_handle.close()
                tmp_file_handle = NamedTemporaryFile(delete=True)
                json_file = open(tmp_file_handle.name, "w")
                json_file.write("[")
                doc_count = 0
                page += 1

        json_file.write("]")
        json_file.close()
        if doc_count == 0 and page == 0:
            self.log.warning(f"Nothing to FULL upload for index {index}")
        else:
            df = pl.read_json(file=tmp_file_handle.name, json_lines=False)
            self._upload_parquet(
                df,
                object_name=f"{self.bucket_dir}/{table_name}/full/{table_name}_{page}.parquet",
            )
        tmp_file_handle.close()

        if self._check_parquet(
            f"{self.bucket_dir}/{table_name}/full/{table_name}_",
        ):
            self._load_parquets_in_bq(
                source_uris=[
                    f"gs://{self.bucket}/{self.bucket_dir}/{table_name}/full/{table_name}_*.parquet"
                ],
                destination_project_dataset_table=f"{self.destination_project_dataset}.{table_name}",
                # cluster_fields=
            )
