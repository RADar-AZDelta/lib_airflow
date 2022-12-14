# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json

import polars as pl


class PolarsSchemaJsonDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    @classmethod
    def object_hook(cls, dct):
        for k, v in dct.items():
            if isinstance(v, str):
                match v:
                    case "integer":
                        dct[k] = pl.UInt32
                    case "string":
                        dct[k] = pl.Utf8
                    case "date":
                        dct[k] = pl.Date
                    case "datetime":
                        dct[k] = pl.Datetime
                    case "bool":
                        dct[k] = pl.Boolean
            elif isinstance(v, dict):
                dct[k] = pl.Struct(v)
        return dct
