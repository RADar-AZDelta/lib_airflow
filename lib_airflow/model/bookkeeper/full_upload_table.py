from typing import Optional

from .table import BookkeeperTable


class BookkeeperFullUploadTable(BookkeeperTable):
    page_size: Optional[int]
    current_pk: Optional[str]
    current_page: Optional[int]
