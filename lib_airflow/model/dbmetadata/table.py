from typing import TypedDict


class Table(TypedDict):
    database: str
    schema: str
    table: str
    pks: list[str]
    pks_type: list[str]
    columns: list[str]
    columns_type: list[str]
