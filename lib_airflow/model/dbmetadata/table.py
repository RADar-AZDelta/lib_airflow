from typing import List, TypedDict


class Table(TypedDict):
    database_name: str
    schema_name: str
    table_name: str
    pks: List[str]
    pks_type: List[str]
    columns: List[str]
    columns_type: List[str]
    is_identity: bool
    # object_id: int
    # is_track_columns_updated_on: bool
    # min_valid_version: int
    # begin_version: int
    # cleanup_version: int
