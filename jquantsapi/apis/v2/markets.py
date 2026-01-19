from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class MktShortRatioApiV2(BaseApi):
    """
    v2 の業種別空売り比率 API (`/markets/short-ratio`) のラッパークラス。
    """

    name = "mkt_short_ratio"
    version = "v2"

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
        `/markets/short-ratio` を実行し、業種別空売り比率データを DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if sector_33_code:
            params["s33"] = sector_33_code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/short-ratio",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "S33"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/short_selling` と同様に、定義済みカラムの順序で返す
        cols = constants.MKT_SHORT_RATIO_COLUMNS_V2
        return df[cols].reset_index(drop=True)


class MktShortSaleReportApiV2(BaseApi):
    """
    v2 の空売り残高報告 API (`/markets/short-sale-report`) のラッパークラス。
    """

    name = "mkt_short_sale_report"
    version = "v2"

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
        `/markets/short-sale-report` を実行し、空売り残高報告データを DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if disclosed_date:
            params["disc_date"] = disclosed_date
        if disclosed_date_from:
            params["disc_date_from"] = disclosed_date_from
        if disclosed_date_to:
            params["disc_date_to"] = disclosed_date_to
        if calculated_date:
            params["calc_date"] = calculated_date

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/short-sale-report",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        for col in ("DiscDate", "CalcDate", "PrevRptDate"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "CalcDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class MktMarginInterestApiV2(BaseApi):
    """
    v2 の信用取引週末残高 API (`/markets/margin-interest`) のラッパークラス。
    """

    name = "mkt_margin_interest"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/margin-interest` を実行し、信用取引週末残高を DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/margin-interest",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class MktBreakdownApiV2(BaseApi):
    """
    v2 の売買内訳 API (`/markets/breakdown`) のラッパークラス。
    """

    name = "mkt_breakdown"
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
        """
        `/markets/breakdown` を実行し、売買内訳データを DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/breakdown",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Code", "Date"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/breakdown` と同様に、定義済みカラムの順序で返す
        cols = constants.MKT_BREAKDOWN_COLUMNS_V2
        return df[cols].reset_index(drop=True)


class MktMarginAlertApiV2(BaseApi):
    """
    v2 の日々公表信用取引残高 API (`/markets/margin-alert`) のラッパークラス。
    """

    name = "mkt_margin_alert"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/margin-alert` を実行し、日々公表信用取引残高を DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/margin-alert",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)


class MktCalendarApiV2(BaseApi):
    """
    v2 の取引カレンダー API (`/markets/calendar`) のラッパークラス。
    """

    name = "markets_trading_calendar"
    version = "v2"

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
        `/markets/calendar` を実行し、取引カレンダーデータを DataFrame で返す。
        """
        params: dict[str, Any] = {}
        if holiday_division:
            params["hol_div"] = holiday_division
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = client._get_paginated(  # type: ignore[attr-defined]
            "/markets/calendar",
            params=params,
        )
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df.sort_values("Date", inplace=True)
        return df.reset_index(drop=True)
