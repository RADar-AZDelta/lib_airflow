import json

from airflow.providers.http.operators.http import SimpleHttpOperator


class JsonHttpOperator(SimpleHttpOperator):
    """
    Airflow operator that decodes the HTTP result text to JSON.
    """
    def execute(self, context):
        text = super(JsonHttpOperator, self).execute(context)
        return json.loads(text)
