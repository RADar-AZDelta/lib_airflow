# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

import json

import pyarrow as pa


class ArrowSchemaJsonDecoder(json.JSONDecoder):
    def __init__(
        self,
        timestamp_unit: str = "ms",
        timestamp_tz: str = "Europe/Brussels",
        *args,
        **kwargs,
    ):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

        self.timestamp_unit = timestamp_unit
        self.timestamp_tz = timestamp_tz

    # @classmethod
    def object_hook(self, dct):
        for k, v in dct.items():
            if isinstance(v, str):
                match v:
                    case "integer":
                        dct[k] = pa.int64()
                    case "string":
                        dct[k] = pa.utf8()
                    case "date":
                        dct[k] = pa.date32()
                    case "datetime":
                        dct[k] = pa.timestamp(
                            unit=self.timestamp_unit, tz=self.timestamp_tz
                        )
                    case "bool":
                        dct[k] = pa.bool_()
                    case "number":
                        dct[k] = pa.float64()
                    case "string[]":
                        dct[k] = pa.list_(pa.utf8())
                    case "integer[]":
                        dct[k] = pa.list_(pa.int64())
                    case "number[]":
                        dct[k] = pa.list_(pa.float64())
                    case _:
                        breakpoint()
            elif isinstance(v, list) and all(
                isinstance(item, pa.Field) for item in v
            ):  # object
                dct[k] = pa.struct(v)
            elif isinstance(v, list) and all(
                isinstance(item, list) for item in v
            ):  # list of objects (we assume all object are of the same type!!!!!)
                dct[k] = pa.list_(pa.struct(v[0]))
            else:
                breakpoint()
        return [pa.field(k, v, nullable=True) for k, v in dct.items()]


class UTCArrowSchemaJsonDecoder(ArrowSchemaJsonDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(timestamp_tz="UTC", *args, **kwargs)
