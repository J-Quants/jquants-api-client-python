from __future__ import annotations

import json
from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class PricesDailyQuotesApiV1(BaseApi):
    """
    v1 の株価四本値 API (`/prices/daily_quotes`) のラッパークラス。
    """

    name = "prices_daily_quotes"
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
        """
        `/prices/daily_quotes` を実行し、株価情報を DataFrame で返す。

        Args:
            client: v1 `Client` インスタンスを想定
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
        """
        url = f"{client.JQUANTS_API_BASE}/prices/daily_quotes"  # type: ignore[attr-defined]
        params: dict[str, Any] = {"code": code}
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd

        data: list[dict[str, Any]] = []
        pagination_key: str = ""
        while True:
            req_params = dict(params)
            if pagination_key != "":
                req_params["pagination_key"] = pagination_key

            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d: dict[str, Any] = json.loads(resp.text)
            page = d.get("daily_quotes", [])
            if isinstance(page, list):
                data.extend(page)

            pagination_key = d.get("pagination_key", "")
            if not pagination_key:
                break

        df = pd.DataFrame.from_dict(data)

        premium_flag = "MorningClose" in df.columns
        if premium_flag:
            cols = constants.PRICES_DAILY_QUOTES_PREMIUM_COLUMNS
        else:
            cols = constants.PRICES_DAILY_QUOTES_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)

        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code", "Date"], inplace=True)
        return df[cols]


class PricesPricesAmApiV1(BaseApi):
    """
    v1 の前場四本値 API (`/prices/prices_am`) のラッパークラス。
    """

    name = "prices_prices_am"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/prices/prices_am` を実行し、前場四本値を DataFrame で返す。

        Args:
            client: v1 `Client` インスタンスを想定
            code: issue code (e.g. 27800 or 2780)
        """
        url = f"{client.JQUANTS_API_BASE}/prices/prices_am"  # type: ignore[attr-defined]
        params: dict[str, Any] = {"code": code}

        resp = client._get(url, params)  # type: ignore[arg-type]
        resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        d: dict[str, Any] = json.loads(resp.text)
        if d.get("message"):
            return d["message"]  # type: ignore[return-value]
        data: list[dict[str, Any]] = d.get("prices_am", [])
        while "pagination_key" in d:
            req_params = dict(params)
            req_params["pagination_key"] = d["pagination_key"]
            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d = json.loads(resp.text)
            data += d.get("prices_am", [])
        df = pd.DataFrame.from_dict(data)
        cols = constants.PRICES_PRICES_AM_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]
