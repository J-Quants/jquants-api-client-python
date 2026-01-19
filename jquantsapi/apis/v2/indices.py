from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore

from jquantsapi.apis.base import BaseApi, SupportsRequest


class IdxBarsDailyApiV2(BaseApi):
    """
    v2 の指数四本値 API (`/indices/bars/daily`) のラッパークラス。

    v1 `/indices` に対応する指数四本値エンドポイント。
    """

    name = "idx-bars-daily"
    version = "v2"

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
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        # v1 と同様: date があれば date 優先、なければ from/to
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = client._get_paginated("/indices/bars/daily", params=params)  # type: ignore[attr-defined]
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            sort_cols: list[str] = []
            if "Code" in df.columns:
                sort_cols.append("Code")
            sort_cols.append("Date")
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class IdxBarsDailyTopixApiV2(BaseApi):
    """
    v2 の TOPIX 指数四本値 API (`/indices/bars/daily/topix`) のラッパークラス。

    v1 `/indices/topix` に対応する TOPIX 指数四本値エンドポイント。
    """

    name = "idx-bars-daily-topix"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: dict[str, Any] = {}
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = client._get_paginated("/indices/bars/daily/topix", params=params)  # type: ignore[attr-defined]
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.sort_values("Date", inplace=True)
        return df.reset_index(drop=True)
