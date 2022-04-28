from typing import Sequence

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator as BigQueryCreateEmptyTableBaseOperator,
)


class BigQueryCreateEmptyTableOperator(BigQueryCreateEmptyTableBaseOperator):
    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "project_id",
        "gcs_schema_object",
        "labels",
        "view",
        "materialized_view",
        "impersonation_chain",
        "schema_fields",
        "cluster_fields",
    )

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
