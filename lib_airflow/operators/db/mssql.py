from typing import Sequence

from airflow.providers.microsoft.mssql.operators.mssql import \
    MsSqlOperator as BaseOperator


class MsSqlOperator(BaseOperator):
    template_fields: Sequence[str] = ('sql', 'parameters')

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
