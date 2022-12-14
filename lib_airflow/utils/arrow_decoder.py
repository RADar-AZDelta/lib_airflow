# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
import re
from datetime import datetime

match_iso8601 = re.compile(
    r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])(T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?)?$"
).match


class ArrowJsonDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    @classmethod
    def object_hook(cls, dct):
        for k, v in dct.items():
            if isinstance(v, str) and match_iso8601(v) is not None:
                try:
                    dct[k] = datetime.fromisoformat(v)
                except:
                    pass
            elif isinstance(v, list) and not len(v):
                dct[k] = None
        return dct
