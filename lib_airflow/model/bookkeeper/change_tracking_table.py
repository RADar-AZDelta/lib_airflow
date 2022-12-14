from typing import Optional, TypedDict

from .full_upload_table import BookkeeperFullUploadTable


class BookkeeperChangeTrackingTable(BookkeeperFullUploadTable):
    version: Optional[int]
    change_tracking_table: Optional[str]
