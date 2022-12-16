# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
from datetime import date, datetime


class AirflowJsonEncoder(json.JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(
            obj,
            (
                bytes,
                bytearray,
            ),
        ):
            return "0x" + obj.hex(":")
