from __future__ import annotations

import json
from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class DerivativesFuturesApiV1(BaseApi):
    """
    v1 の先物四本値 API (`/derivatives/futures`) のラッパークラス。
    """

    name = "derivatives_futures"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str = "",
        category: str = "",
        contract_flag: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        # 元の _get_derivatives_futures_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/derivatives/futures"  # type: ignore[attr-defined]
        params = {
            "category": category,
            "date": date_yyyymmdd,
            "contract_flag": contract_flag,
        }

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data = d["futures"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["futures"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.DERIVATIVES_FUTURES_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]


class DerivativesOptionsApiV1(BaseApi):
    """
    v1 のオプション四本値 API (`/derivatives/options`) のラッパークラス。
    """

    name = "derivatives_options"
    version = "v1"

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
        # 元の _get_derivatives_options_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/derivatives/options"  # type: ignore[attr-defined]
        params = {
            "category": category,
            "date": date_yyyymmdd,
            "contract_flag": contract_flag,
            "code": code,
        }

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data = d["options"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["options"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.DERIVATIVES_OPTIONS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]


class OptionIndexOptionApiV1(BaseApi):
    """
    v1 のオプション指数四本値 API (`/option/index_option`) のラッパークラス。
    """

    name = "option_index_option"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        # 元の _get_option_index_option_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/option/index_option"  # type: ignore[attr-defined]
        params = {"date": date_yyyymmdd}

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data = d["index_option"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["index_option"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.OPTION_INDEX_OPTION_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]
