# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Sequence

from airflow.providers.microsoft.mssql.operators.mssql import (
    MsSqlOperator as MsSqlBaseOperator,
)


class MsSqlOperator(MsSqlBaseOperator):
    """MsSqlOperator with additional template fields"""

    template_fields: Sequence[str] = ("sql", "parameters")

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
