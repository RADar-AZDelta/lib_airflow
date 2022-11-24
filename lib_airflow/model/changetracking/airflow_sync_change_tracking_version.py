from datetime import datetime
from typing import Optional, TypedDict


class AirflowSyncChangeTrackingVersion(TypedDict):
    database: str
    schema: str
    table: str
    page_size: Optional[int]
    change_tracking_table: Optional[str]
    disabled: Optional[bool]
    version: Optional[int]
    bulk_upload_page: Optional[int]
    current_identity_value: Optional[int]
    current_single_nvarchar_pk: Optional[str]
