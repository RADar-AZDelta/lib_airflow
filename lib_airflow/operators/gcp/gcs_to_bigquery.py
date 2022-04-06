from typing import Sequence

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator as GCSToBigQueryBaseOperator,
)


class GCSToBigQueryOperator(GCSToBigQueryBaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "source_objects",
        "schema_object",
        "destination_project_dataset_table",
        "impersonation_chain",
        "cluster_fields",
    )

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
