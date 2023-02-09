# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json

import pyarrow as pa


class ArrowSchemaJsonDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    @classmethod
    def object_hook(cls, dct):
        for k, v in dct.items():
            if isinstance(v, str):
                match v:
                    case "integer":
                        dct[k] = pa.uint64()
                    case "string":
                        dct[k] = pa.utf8()
                    case "date":
                        dct[k] = pa.date32()
                    case "datetime":
                        dct[k] = pa.timestamp("ms", tz="Europe/Brussels")
                    case "bool":
                        dct[k] = pa.bool_()
                    case "number":
                        dct[k] = pa.float64()
            elif isinstance(v, list) and all(isinstance(item, tuple) for item in v):
                dct[k] = pa.struct(v)
        return [(k, v) for k, v in dct.items()]
