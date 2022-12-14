from typing import Optional, TypedDict


class BookkeeperTable(TypedDict):
    database: str
    schema: str
    table: str
    disabled: Optional[bool]
