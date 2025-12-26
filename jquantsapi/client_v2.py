import os
import platform
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from jquantsapi import __version__


class ClientV2:
    """
    J-Quants API v2 用のクライアント

    - 認証方式: x-api-key (APIキー)
      - ダッシュボードで発行した API キーを `api_key` 引数、もしくは
        環境変数 `JQUANTS_API_KEY` で指定してください。
    - v1 版 `Client` と同様に、各エンドポイントの結果を pandas.DataFrame で返しますが、
      v2 ではレスポンスのフィールド名が変更されているため、列名は v1 と異なります。
    """

    JQUANTS_API_BASE = "https://api.jquants.com/v2"
    USER_AGENT = "jqapi-python-v2"
    USER_AGENT_VERSION = __version__
    RAW_ENCODING = "utf-8"

    def __init__(self, api_key: Optional[str] = None) -> None:
        """
        Args:
            api_key: J-Quants API v2 の API キー
                     未指定の場合は環境変数 `JQUANTS_API_KEY` を参照します。
        """
        if api_key is None:
            api_key = os.environ.get("JQUANTS_API_KEY", "")

        if not api_key:
            raise ValueError(
                "api_key is required. Set it via argument or JQUANTS_API_KEY env var."
            )

        self._api_key = api_key
        self._session: Optional[requests.Session] = None

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _request_session(
        self,
        status_forcelist: Optional[List[int]] = None,
        allowed_methods: Optional[List[str]] = None,
    ) -> requests.Session:
        """
        requests の session を取得し、リトライ設定を行う
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
        if allowed_methods is None:
            allowed_methods = ["HEAD", "GET", "OPTIONS"]

        if self._session is None:
            retry_strategy = Retry(
                total=3,
                status_forcelist=status_forcelist,
                allowed_methods=allowed_methods,
            )
            adapter = HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=retry_strategy,
            )
            self._session = requests.Session()
            self._session.mount("https://", adapter)

        return self._session

    def _base_headers(self) -> Dict[str, str]:
        """
        J-Quants API v2 にアクセスする際の共通ヘッダを生成
        """
        return {
            "x-api-key": self._api_key,
            "User-Agent": f"{self.USER_AGENT}/{self.USER_AGENT_VERSION} "
            f"p/{platform.python_version()}",
        }

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """
        GET リクエスト用ラッパー
        """
        session = self._request_session()
        headers = self._base_headers()
        resp = session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp

    def _get_paginated(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data_key: str = "data",
    ) -> List[Dict[str, Any]]:
        """
        pagination_key に対応した共通 GET ヘルパー

        Args:
            path: ベースURLからのパス (例: \"/equities/master\")
            params: クエリパラメータ
            data_key: レスポンス中のデータ配列キー (デフォルト: \"data\")
        Returns:
            List[Dict[str, Any]]: 連結済みのデータ配列
        """
        url = f"{self.JQUANTS_API_BASE}{path}"
        all_data: List[Dict[str, Any]] = []
        query: Dict[str, Any] = dict(params or {})

        while True:
            resp = self._get(url, params=query)
            payload = resp.json()
            batch = payload.get(data_key, [])
            if isinstance(batch, list):
                all_data.extend(batch)

            pagination_key = payload.get("pagination_key")
            if not pagination_key:
                break
            query["pagination_key"] = pagination_key

        return all_data

    # ------------------------------------------------------------------
    # /equities/master (path_old: /listed/info)
    # ------------------------------------------------------------------
    def get_listed_info(
        self,
        code: str = "",
        date: str = "",
    ) -> pd.DataFrame:
        """
        上場銘柄一覧 (v2: /equities/master)

        v1 の `get_listed_info` と同等の目的ですが、カラム名は v2 仕様
        (例: CoName, CoNameEn, S17 など) になります。

        Args:
            code: 5桁の銘柄コード (例: 27800)。4桁指定も可能。
            date: 基準日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 上場銘柄情報
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date

        data = self._get_paginated("/equities/master", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values("Code", inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /equities/bars/daily (path_old: /prices/daily_quotes)
    # ------------------------------------------------------------------
    def get_prices_daily_quotes(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        株価四本値 (v2: /equities/bars/daily)

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 株価データ (v2のフィールド名で返却)
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/equities/bars/daily", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Code", "Date"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /equities/bars/daily/am (path_old: /prices/prices_am)
    # ------------------------------------------------------------------
    def get_prices_prices_am(self, code: str = "") -> pd.DataFrame:
        """
        前場四本値 (v2: /equities/bars/daily/am)

        Args:
            code: 銘柄コード (5桁 or 4桁)。空文字の場合は全銘柄。
        Returns:
            pd.DataFrame: 前場の株価データ
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code

        data = self._get_paginated("/equities/bars/daily/am", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values("Code", inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /equities/investor-types (path_old: /markets/trades_spec)
    # ------------------------------------------------------------------
    def get_markets_trades_spec(
        self,
        section: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        投資部門別売買状況 (v2: /equities/investor-types)

        Args:
            section: 市場区分 (例: \"TSEPrime\")
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        Returns:
            pd.DataFrame: 投資部門別売買データ
        """
        params: Dict[str, Any] = {}
        if section:
            params["section"] = section
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = self._get_paginated("/equities/investor-types", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Section"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/summary (path_old: /fins/statements)
    # ------------------------------------------------------------------
    def get_fins_statements(
        self,
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        財務情報サマリ (v2: /fins/summary)

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 財務情報 (v2のフィールド名で返却)
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        data = self._get_paginated("/fins/summary", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        for col in ("DiscDate", "CurPerSt", "CurPerEn", "CurFYSt", "CurFYEn", "NxtFYSt", "NxtFYEn"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/details (path_old: /fins/fs_details)
    # ------------------------------------------------------------------
    def get_fins_fs_details(
        self,
        code: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        財務諸表詳細 (v2: /fins/details)

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 財務諸表詳細 (FS列に各項目が含まれる)
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd

        data = self._get_paginated("/fins/details", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "DiscDate" in df.columns:
            df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /fins/dividend (path_old: /fins/dividend)
    # ------------------------------------------------------------------
    def get_fins_dividend(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        配当金情報 (v2: /fins/dividend)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 配当金データ
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/fins/dividend", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /equities/earnings-calendar (path_old: /fins/announcement)
    # ------------------------------------------------------------------
    def get_fins_announcement(self) -> pd.DataFrame:
        """
        決算発表予定日 (v2: /equities/earnings-calendar)

        Returns:
            pd.DataFrame: 決算発表予定データ
        """
        data = self._get_paginated("/equities/earnings-calendar", params={})
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/short-ratio (path_old: /markets/short_selling)
    # ------------------------------------------------------------------
    def get_markets_short_selling(
        self,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        業種別空売り比率 (v2: /markets/short-ratio)

        Args:
            sector_33_code: 33業種コード (例: 0050)
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 業種別空売り比率データ
        """
        params: Dict[str, Any] = {}
        if sector_33_code:
            params["s33"] = sector_33_code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/markets/short-ratio", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "S33"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/short-sale-report (path_old: /markets/short_selling_positions)
    # ------------------------------------------------------------------
    def get_markets_short_selling_positions(
        self,
        code: str = "",
        disclosed_date: str = "",
        disclosed_date_from: str = "",
        disclosed_date_to: str = "",
        calculated_date: str = "",
    ) -> pd.DataFrame:
        """
        空売り残高報告 (v2: /markets/short-sale-report)

        Args:
            code: 銘柄コード
            disclosed_date: 開示日
            disclosed_date_from: 開示日(開始)
            disclosed_date_to: 開示日(終了)
            calculated_date: 算出日
        Returns:
            pd.DataFrame: 空売り残高報告データ
        """
        params: Dict[str, Any] = {}
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

        data = self._get_paginated("/markets/short-sale-report", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        for col in ("DiscDate", "CalcDate", "PrevRptDate"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "CalcDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/margin-interest (path_old: /markets/weekly_margin_interest)
    # ------------------------------------------------------------------
    def get_markets_weekly_margin_interest(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        信用取引週末残高 (v2: /markets/margin-interest)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 信用取引週末残高データ
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/markets/margin-interest", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sort_cols = [c for c in ["Date", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/margin-alert (path_old: /markets/daily_margin_interest)
    # ------------------------------------------------------------------
    def get_markets_daily_margin_interest(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        日々公表信用取引残高 (v2: /markets/margin-alert)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 日々公表信用取引残高データ
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/markets/margin-alert", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["Code", "PubDate"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/breakdown (path_old: /markets/breakdown)
    # ------------------------------------------------------------------
    def get_markets_breakdown(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        売買内訳データ (v2: /markets/breakdown)

        Args:
            code: 銘柄コード
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
            date_yyyymmdd: 特定日付
        Returns:
            pd.DataFrame: 売買内訳データ
        """
        params: Dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd:
                params["from"] = from_yyyymmdd
            if to_yyyymmdd:
                params["to"] = to_yyyymmdd

        data = self._get_paginated("/markets/breakdown", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Code" in df.columns:
            df.sort_values(["Code", "Date"], inplace=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/calendar (path_old: /markets/trading_calendar)
    # ------------------------------------------------------------------
    def get_markets_trading_calendar(
        self,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        取引カレンダー (v2: /markets/calendar)

        Args:
            holiday_division: 休日区分 (HolDiv コード)
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        Returns:
            pd.DataFrame: 取引カレンダーデータ
        """
        params: Dict[str, Any] = {}
        if holiday_division:
            params["hol_div"] = holiday_division
        if from_yyyymmdd:
            params["from"] = from_yyyymmdd
        if to_yyyymmdd:
            params["to"] = to_yyyymmdd

        data = self._get_paginated("/markets/calendar", params=params)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if "Date" in df.columns:
            df.sort_values("Date", inplace=True)
        return df.reset_index(drop=True)


