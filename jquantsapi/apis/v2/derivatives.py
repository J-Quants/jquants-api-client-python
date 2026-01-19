from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore

from jquantsapi.apis.base import BaseApi, SupportsRequest


class DrvBarsDailyFutApiV2(BaseApi):
    """
    v2 の先物四本値 API (`/derivatives/bars/daily/futures`) のラッパークラス。

    v1 `/derivatives/futures` に対応する先物四本値エンドポイント。
    """

    name = "drv-bars-daily-fut"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str = "",
        category: str = "",
        contract_flag: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"date": date_yyyymmdd}
        if category:
            params["category"] = category
        if contract_flag:
            params["contract_flag"] = contract_flag

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/derivatives/bars/daily/futures", params=params
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class DrvBarsDailyOptApiV2(BaseApi):
    """
    v2 のオプション四本値 API (`/derivatives/bars/daily/options`) のラッパークラス。

    v1 `/derivatives/options` に対応するオプション四本値エンドポイント。
    """

    name = "drv-bars-daily-opt"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str = "",
        category: str = "",
        contract_flag: str = "",
        code: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"date": date_yyyymmdd}
        if category:
            params["category"] = category
        if contract_flag:
            params["contract_flag"] = contract_flag
        if code:
            params["code"] = code

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/derivatives/bars/daily/options",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class DrvBarsDailyOpt225ApiV2(BaseApi):
    """
    v2 の日経225オプション四本値 API (`/derivatives/bars/daily/options/225`) のラッパークラス。

    v1 `/option/index_option` に対応する日経225オプション四本値エンドポイント。
    """

    name = "drv-bars-daily-opt-225"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"date": date_yyyymmdd}

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/derivatives/bars/daily/options/225",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols: list[str] = []
        if "Code" in df.columns:
            sort_cols.append("Code")
        if "Date" in df.columns:
            sort_cols.append("Date")
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)
