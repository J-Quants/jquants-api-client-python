from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class EqMasterApiV2(BaseApi):
    """
    v2 の銘柄マスタ API (`/equities/master`) のラッパークラス。

    上場銘柄一覧 (v2) を取得します。
    """

    name = "eq_master"
    version = "v2"

    def execute(
        self, client: Any, code: str = "", date: str = "", **kwargs: Any
    ) -> pd.DataFrame:
        params: dict[str, str] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date

        # ClientV2 の _get_paginated を利用する
        data: list[dict[str, Any]] = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/master", params=params, data_key="data"
        )
        cols = constants.EQ_MASTER_COLUMNS_V2
        if not data:
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values("Code", inplace=True)

        return df[cols].reset_index(drop=True)


class EqBarsDailyApiV2(BaseApi):
    """
    v2 の株価四本値 API (`/equities/bars/daily`) のラッパークラス。
    """

    name = "eq_bars_daily"
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
        v2 `/equities/bars/daily` を実行し、株価四本値を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
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

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/bars/daily",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        sort_cols = [c for c in ["Code", "Date"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        return df.reset_index(drop=True)


class EqBarsDailyAmApiV2(BaseApi):
    """
    v2 の前場四本値 API (`/equities/bars/daily/am`) のラッパークラス。
    """

    name = "eq_bars_daily_am"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        v2 `/equities/bars/daily/am` を実行し、前場四本値を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            code: 銘柄コード (5桁 or 4桁)。空文字の場合は全銘柄。
        """
        params: dict[str, Any] = {}
        if code:
            params["code"] = code

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/bars/daily/am",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values(["Code", "Date"], inplace=True)

        # v1 `/prices/prices_am` と同様に、前場四本値に対応する列のみを返す
        cols = constants.PRICES_PRICES_AM_COLUMNS_V2
        return df[cols].reset_index(drop=True)


class EqBarsMinuteApiV2(BaseApi):
    """
    v2 の分足 API (`/equities/bars/minute`) のラッパークラス。
    """

    name = "eq_bars_minute"
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
        v2 `/equities/bars/minute` を実行し、分足を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
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

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/bars/minute",
            params=params,
        )

        cols = constants.EQ_BARS_MINUTE_COLUMNS_V2
        if not all_data:
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        sort_cols = [c for c in ["Code", "Date", "Time"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        return df[cols].reset_index(drop=True)


class EqInvestorTypesApiV2(BaseApi):
    """
    v2 の投資部門別売買状況 API (`/equities/investor-types`) のラッパークラス。
    """

    name = "eq_investor_types"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        section: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        v2 `/equities/investor-types` を実行し、投資部門別売買状況を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            section: 市場区分 (例: "TSEPrime")
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        """
        params: dict[str, Any] = {}
        if section:
            params["section"] = section
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/investor-types",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Section"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        # v1 `/markets/trades_spec` と同様に、定義済みカラムの順序で返す
        cols = constants.EQ_INVESTOR_TYPES_COLUMNS_V2
        return df[cols].reset_index(drop=True)


class EqEarningsCalApiV2(BaseApi):
    """
    v2 の決算発表予定日 API (`/equities/earnings-calendar`) のラッパークラス。
    """

    name = "eq_earnings_cal"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/equities/earnings-calendar` を実行し、決算発表予定データを DataFrame で返す。
        """
        params: dict[str, Any] = {}

        all_data = client._get_paginated(  # type: ignore[attr-defined]
            "/equities/earnings-calendar",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)
