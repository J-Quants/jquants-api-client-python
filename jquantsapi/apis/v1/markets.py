from __future__ import annotations

import json
from typing import Any, Union

import pandas as pd  # type: ignore

from jquantsapi import constants, enums
from jquantsapi.apis.base import BaseApi, SupportsRequest


class MarketsTradesSpecApiV1(BaseApi):
    """
    v1 の投資部門別売買状況 API (`/markets/trades_spec`) のラッパークラス。
    """

    name = "markets_trades_spec"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        section: Union[str, enums.MARKET_API_SECTIONS] = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/trades_spec` を実行し、投資部門別売買状況を DataFrame で返す。

        Args:
            client: v1 `Client` インスタンスを想定
            section: section name (e.g. "TSEPrime" or MARKET_API_SECTIONS.TSEPrime)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        """
        # 元の _get_markets_trades_spec_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/trades_spec"  # type: ignore[attr-defined]
        params = {}
        if section != "":
            params["section"] = section
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data: list[dict[str, Any]] = d["trades_spec"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["trades_spec"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKETS_TRADES_SPEC
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["PublishedDate"] = pd.to_datetime(df["PublishedDate"], format="%Y-%m-%d")
        df["StartDate"] = pd.to_datetime(df["StartDate"], format="%Y-%m-%d")
        df["EndDate"] = pd.to_datetime(df["EndDate"], format="%Y-%m-%d")
        df.sort_values(["PublishedDate", "Section"], inplace=True)
        return df[cols]


class MarketsWeeklyMarginInterestApiV1(BaseApi):
    """
    v1 の信用取引週末残高 API (`/markets/weekly_margin_interest`) のラッパークラス。
    """

    name = "markets_weekly_margin_interest"
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
        `/markets/weekly_margin_interest` を実行し、信用取引週末残高を DataFrame で返す。
        """
        # 元の _get_markets_weekly_margin_interest_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/weekly_margin_interest"  # type: ignore[attr-defined]
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
        data: list[dict[str, Any]] = d["weekly_margin_interest"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["weekly_margin_interest"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKETS_WEEKLY_MARGIN_INTEREST
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]


class MarketsTradingCalendarApiV1(BaseApi):
    """
    v1 の取引カレンダー API (`/markets/trading_calendar`) のラッパークラス。
    """

    name = "markets_trading_calendar"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/trading_calendar` を実行し、取引カレンダーデータを DataFrame で返す。
        """
        # 元の _get_markets_trading_calendar_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/trading_calendar"  # type: ignore[attr-defined]
        params = {}
        if holiday_division != "":
            params["holidaydivision"] = holiday_division
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        df = pd.DataFrame.from_dict(d["trading_calendar"])
        cols = constants.MARKETS_TRADING_CALENDAR
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]


class MarketsShortSellingApiV1(BaseApi):
    """
    v1 の業種別空売り比率 API (`/markets/short_selling`) のラッパークラス。
    """

    name = "markets_short_selling"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/short_selling` を実行し、業種別空売り比率データを DataFrame で返す。
        """
        # 元の _get_markets_short_selling_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/short_selling"  # type: ignore[attr-defined]
        params = {"sector33code": sector_33_code}
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
        data: list[dict[str, Any]] = d["short_selling"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["short_selling"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKET_SHORT_SELLING_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Sector33Code"], inplace=True)
        return df[cols]


class MarketsBreakdownApiV1(BaseApi):
    """
    v1 の売買内訳 API (`/markets/breakdown`) のラッパークラス。
    """

    name = "markets_breakdown"
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
        `/markets/breakdown` を実行し、売買内訳データを DataFrame で返す。
        """
        # 元の _get_markets_breakdown_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/breakdown"  # type: ignore[attr-defined]
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
        data: list[dict[str, Any]] = d["breakdown"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["breakdown"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKETS_BREAKDOWN_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]


class MarketsShortSellingPositionsApiV1(BaseApi):
    """
    v1 の空売り残高報告 API (`/markets/short_selling_positions`) のラッパークラス。
    """

    name = "markets_short_selling_positions"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        disclosed_date: str = "",
        disclosed_date_from: str = "",
        disclosed_date_to: str = "",
        calculated_date: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/short_selling_positions` を実行し、空売り残高報告データを DataFrame で返す。
        """
        # 元の _get_markets_short_selling_positions_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/short_selling_positions"  # type: ignore[attr-defined]
        params = {}
        if code != "":
            params["code"] = code
        if disclosed_date != "":
            params["disclosed_date"] = disclosed_date
        if disclosed_date_from != "":
            params["disclosed_date_from"] = disclosed_date_from
        if disclosed_date_to != "":
            params["disclosed_date_to"] = disclosed_date_to
        if calculated_date != "":
            params["calculated_date"] = calculated_date

        ret = client._get(url, params)  # type: ignore[attr-defined]
        ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
        j = ret.text
        d: dict[str, Any] = json.loads(j)
        data: list[dict[str, Any]] = d["short_selling_positions"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["short_selling_positions"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.SHORT_SELLING_POSITIONS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["DisclosedDate"] = pd.to_datetime(
            df["DisclosedDate"], format="%Y-%m-%d", errors="coerce"
        )
        df["CalculatedDate"] = pd.to_datetime(
            df["CalculatedDate"], format="%Y-%m-%d", errors="coerce"
        )
        df["CalculationInPreviousReportingDate"] = pd.to_datetime(
            df["CalculationInPreviousReportingDate"], format="%Y-%m-%d", errors="coerce"
        )
        df.sort_values(["DisclosedDate", "CalculatedDate", "Code"], inplace=True)
        return df[cols]


class MarketsDailyMarginInterestApiV1(BaseApi):
    """
    v1 の日々公表信用取引残高 API (`/markets/daily_margin_interest`) のラッパークラス。
    """

    name = "markets_daily_margin_interest"
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
        `/markets/daily_margin_interest` を実行し、日々公表信用取引残高を DataFrame で返す。
        """
        # 元の _get_markets_daily_margin_interest_raw の実装を統合
        url = f"{client.JQUANTS_API_BASE}/markets/daily_margin_interest"  # type: ignore[attr-defined]
        params = {}
        if code != "":
            params["code"] = code
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
        data: list[dict[str, Any]] = d["daily_margin_interest"]
        while "pagination_key" in d:
            params["pagination_key"] = d["pagination_key"]
            ret = client._get(url, params)  # type: ignore[attr-defined]
            ret.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            j = ret.text
            d = json.loads(j)
            data += d["daily_margin_interest"]
        df = pd.json_normalize(data=data)
        cols = constants.DAILY_MARGIN_INTEREST_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["PublishedDate"] = pd.to_datetime(df["PublishedDate"], format="%Y-%m-%d")
        df["ApplicationDate"] = pd.to_datetime(df["ApplicationDate"], format="%Y-%m-%d")
        df.sort_values(["Code", "PublishedDate"], inplace=True)
        return df[cols]
