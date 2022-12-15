from typing import Optional, TypedDict

from .full_upload_table import BookkeeperFullUploadTable


class BookkeeperHixDxhTable(BookkeeperFullUploadTable):
    cycleid: Optional[int]
