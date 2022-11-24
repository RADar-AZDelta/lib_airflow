from datetime import datetime
from typing import TypedDict


class PrimaryKeyColumn(TypedDict):
    pk_col_name: str
    is_identity: bool
