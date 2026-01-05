from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class DerivativesFuturesApiV1(BaseApi):
    """
    v1 `/derivatives/futures` のラッパ。

    既存の `Client.get_derivatives_futures` のロジックをそのまま移植する。
    """

    name = "derivatives_futures"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        j = client._get_derivatives_futures_raw(  # type: ignore[attr-defined]
            category=category,
            date_yyyymmdd=date_yyyymmdd,
            contract_flag=contract_flag,
        )
        d: Dict[str, Any] = json.loads(j)
        data = d["futures"]
        while "pagination_key" in d:
            j = client._get_derivatives_futures_raw(  # type: ignore[attr-defined]
                category=category,
                date_yyyymmdd=date_yyyymmdd,
                contract_flag=contract_flag,
                pagination_key=d["pagination_key"],
            )
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
    v1 `/derivatives/options` のラッパ。

    既存の `Client.get_derivatives_options` のロジックをそのまま移植する。
    """

    name = "derivatives_options"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        j = client._get_derivatives_options_raw(  # type: ignore[attr-defined]
            category=category,
            date_yyyymmdd=date_yyyymmdd,
            contract_flag=contract_flag,
            code=code,
        )
        d: Dict[str, Any] = json.loads(j)
        data = d["options"]
        while "pagination_key" in d:
            j = client._get_derivatives_options_raw(  # type: ignore[attr-defined]
                category=category,
                date_yyyymmdd=date_yyyymmdd,
                contract_flag=contract_flag,
                code=code,
                pagination_key=d["pagination_key"],
            )
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
    v1 `/option/index_option` のラッパ。

    既存の `Client.get_option_index_option` のロジックをそのまま移植する。
    """

    name = "option_index_option"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date_yyyymmdd: str,
    ) -> pd.DataFrame:
        j = client._get_option_index_option_raw(  # type: ignore[attr-defined]
            date_yyyymmdd=date_yyyymmdd,
        )
        d: Dict[str, Any] = json.loads(j)
        data = d["index_option"]
        while "pagination_key" in d:
            j = client._get_option_index_option_raw(  # type: ignore[attr-defined]
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["index_option"]

        df = pd.DataFrame.from_dict(data)
        cols = constants.OPTION_INDEX_OPTION_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]

