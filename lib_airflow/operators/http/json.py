# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
from typing import Any

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.context import Context


class JsonHttpOperator(SimpleHttpOperator):
    """
    Airflow operator that decodes the HTTP result text (JSON) into a Python object.
    """

    def execute(self, context: "Context") -> Any:
        """Converts the HTTP result text (JSON) into a Python object.

        Args:
            context (Context): Jinja2 template context for task rendering.

        Returns:
            Any: A Python object representing the JSON
        """
        text = super(JsonHttpOperator, self).execute(context)
        return json.loads(text)
