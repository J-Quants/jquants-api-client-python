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
    v2 の信用取引残高 API (`/markets/margin-interest`) のラッパークラス。

    2026-09-28 リリースの仕様変更後の 16 項目（PubDate 先頭）を返す。
    日次データ・PubDate・金額 6 項目は 2026-09-25 申込分以降のみ値が入り、
    2026-09-24 以前は週末時点（通常は金曜日付）のデータのみで PubDate・金額は null。
    null 列やキー欠落があっても列定義と順序を保つため、他の markets 系 API の
    `df[cols]` ではなく `reindex(columns=cols)` で整形する。
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
        published_date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/markets/margin-interest` を実行し、信用取引残高を DataFrame で返す。

        code / date_yyyymmdd / published_date_yyyymmdd のいずれかの指定が必須です (API 仕様)。
        published_date_yyyymmdd（公表日）は申込日付軸 (date_yyyymmdd / from_yyyymmdd / to_yyyymmdd)
        と同時に指定できません (API は 400 を返す)。code との併用は可能です。
        from_yyyymmdd / to_yyyymmdd で期間を指定する場合は code の指定も必須です
        (仕様書のパラメータ組み合わせに code なしの期間指定が存在しないため。
        片側のみの指定も code があれば通す)。
        date_yyyymmdd を指定した場合、from_yyyymmdd / to_yyyymmdd は無視されます
        (他の API ラッパーと同じ挙動)。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            code: 銘柄コード (5桁 or 4桁)
            date_yyyymmdd: 申込日付
            from_yyyymmdd: 申込日付の取得開始日
            to_yyyymmdd: 申込日付の取得終了日
            published_date_yyyymmdd: 公表日
        """
        if not code and not date_yyyymmdd and not published_date_yyyymmdd:
            raise ValueError(
                "code, date_yyyymmdd, published_date_yyyymmdd のいずれかを指定してください。"
            )
        if published_date_yyyymmdd and (date_yyyymmdd or from_yyyymmdd or to_yyyymmdd):
            raise ValueError(
                "published_date_yyyymmdd は date_yyyymmdd / from_yyyymmdd / to_yyyymmdd "
                "と同時に指定できません。"
            )
        if (from_yyyymmdd or to_yyyymmdd) and not code:
            raise ValueError(
                "from_yyyymmdd / to_yyyymmdd を指定する場合は code も指定してください。"
            )

        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if published_date_yyyymmdd:
            params["published_date"] = published_date_yyyymmdd
        elif date_yyyymmdd:
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

        cols = constants.MKT_MARGIN_INTEREST_COLUMNS_V2
        if not data:
            return pd.DataFrame(columns=cols)

        # 定義済みカラムの順序 (PubDate 先頭・16 項目) に整形する。
        # 仕様外のキーは破棄し、欠落キーは NaN 列として補う
        df = pd.DataFrame.from_records(data).reindex(columns=cols)
        # PubDate は 2026-09-24 以前の過去分では null（NaT）。キー欠落時も
        # datetime64 に揃えるため reindex 後に変換する
        for col in ["PubDate", "Date"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        # ソートは申込日付軸（Date, Code）で固定する。公表日検索時も同じ
        df.sort_values(["Date", "Code"], inplace=True)
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
