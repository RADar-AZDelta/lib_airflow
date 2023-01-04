# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import re
from datetime import datetime

match_iso8601 = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
).match

match_hex = re.compile(r"^0[xX]((:?)[0-9a-fA-F]{2})+$").match


class AirflowJsonDecoder(json.JSONDecoder):
    """
    Custom JSON decoder for Airflow, that correctly decodes iso8601 dates.
    """

    def __init__(self, *args, **kwargs):
        if "object_hook" in kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(object_hook=self.object_hook, *args, **kwargs)

    @classmethod
    def object_hook(cls, dct):
        """
        object_hook is an optional function that will be called with the result of any object literal decoded (a dict).
        The return value of object_hook will be used instead of the dict.
        This feature can be used to implement custom decoders.
        """
        for k, v in dct.items():
            if isinstance(v, str):
                if match_iso8601(v):
                    try:
                        dct[k] = datetime.fromisoformat(v)
                    except ValueError:
                        pass
                elif match_hex(v):
                    try:
                        hex = v[2:].replace(":", "")
                        dct[k] = bytes.fromhex(hex)
                    except:
                        pass
        return dct
