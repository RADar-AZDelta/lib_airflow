# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json
from typing import Literal

import polars as pl


class PolarsSchemaJsonDecoder(json.JSONDecoder):
    def __init__(
        self,
        time_unit: Literal["ns", "us", "ms"] = "ms",
        time_zone: str = "Europe/Brussels",
        *args,
        **kwargs,
    ):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

        self.time_unit = time_unit
        self.time_zone = time_zone

    def object_hook(self, dct):
        for k, v in dct.items():
            if isinstance(v, str):
                match v:
                    case "integer":
                        dct[k] = pl.Int64
                    case "string":
                        dct[k] = pl.Utf8
                    case "date":
                        dct[k] = pl.Date
                    case "datetime":
                        dct[k] = pl.Datetime(
                            time_unit=self.time_unit, time_zone=self.time_zone
                        )
                    case "bool":
                        dct[k] = pl.Boolean
                    case "number":
                        dct[k] = pl.Float64
                    case "string[]":
                        dct[k] = pl.List(pl.Utf8)
                    case "integer[]":
                        dct[k] = pl.List(pl.Int64)
                    case "number[]":
                        dct[k] = pl.List(pl.Float64)
                    case _:
                        breakpoint()
            elif isinstance(v, dict):  # object
                dct[k] = pl.Struct(v)
            elif isinstance(v, list) and all(
                isinstance(item, dict) for item in v
            ):  # list of objects (we assume all object are of the same type!!!!!)
                dct[k] = pl.List(pl.Struct(v[0]))
            else:
                breakpoint()
        return dct


class UTCPolarsSchemaJsonDecoder(PolarsSchemaJsonDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(time_zone="UTC", *args, **kwargs)
