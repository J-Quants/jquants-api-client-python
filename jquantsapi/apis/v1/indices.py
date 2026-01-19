from __future__ import annotations

import json
from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class IndicesApiV1(BaseApi):
    """
    v1 の指数四本値 API (`/indices`) のラッパークラス。
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
        **kwargs: Any,
    ) -> pd.DataFrame:
        # 元の _get_indices_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/indices"  # type: ignore[attr-defined]
        params = {"code": code}
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data = d["indices"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
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
    v1 の TOPIX 指数四本値 API (`/indices/topix`) のラッパークラス。
    """

    name = "indices_topix"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        # 元の _get_indices_topix_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/indices/topix"  # type: ignore[attr-defined]
        params = {}
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data = d["topix"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["topix"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.INDICES_TOPIX_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]
