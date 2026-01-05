from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class IndicesApiV1(BaseApi):
    """
    v1 `/indices` のラッパ。

    既存の `Client.get_indices` のロジックをそのまま移植する。
    """

    name = "indices"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        j = client._get_indices_raw(  # type: ignore[attr-defined]
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d: Dict[str, Any] = json.loads(j)
        data = d["indices"]
        while "pagination_key" in d:
            j = client._get_indices_raw(  # type: ignore[attr-defined]
                code=code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["indices"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.INDICES_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code", "Date"], inplace=True)
        return df[cols]


class IndicesTopixApiV1(BaseApi):
    """
    v1 `/indices/topix` のラッパ。

    既存の `Client.get_indices_topix` のロジックをそのまま移植する。
    """

    name = "indices_topix"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        j = client._get_indices_topix_raw(  # type: ignore[attr-defined]
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )
        d: Dict[str, Any] = json.loads(j)
        data = d["topix"]
        while "pagination_key" in d:
            j = client._get_indices_topix_raw(  # type: ignore[attr-defined]
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["topix"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.INDICES_TOPIX_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]

