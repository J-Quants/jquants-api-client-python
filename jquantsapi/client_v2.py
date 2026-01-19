import os
import platform
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd  # type: ignore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from jquantsapi import __version__, constants
from jquantsapi.apis.v2.bulk import BulkGetApiV2, BulkListApiV2
from jquantsapi.apis.v2.derivatives import (
    DrvBarsDailyFutApiV2,
    DrvBarsDailyOpt225ApiV2,
    DrvBarsDailyOptApiV2,
)
from jquantsapi.apis.v2.equities import (
    EqBarsDailyAmApiV2,
    EqBarsDailyApiV2,
    EqBarsMinuteApiV2,
    EqEarningsCalApiV2,
    EqInvestorTypesApiV2,
    EqMasterApiV2,
)
from jquantsapi.apis.v2.fins import FinDetailsApiV2, FinDividendApiV2, FinSummaryApiV2
from jquantsapi.apis.v2.indices import IdxBarsDailyApiV2, IdxBarsDailyTopixApiV2
from jquantsapi.apis.v2.markets import (
    MktBreakdownApiV2,
    MktCalendarApiV2,
    MktMarginAlertApiV2,
    MktMarginInterestApiV2,
    MktShortRatioApiV2,
    MktShortSaleReportApiV2,
)
from jquantsapi.enums import BulkEndpoint

DatetimeLike = Union[datetime, pd.Timestamp, str]


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
    MAX_WORKERS = 5

    def __init__(self, api_key: Optional[str] = None) -> None:
        """
        Args:
            api_key: J-Quants API v2 の API キー
                     未指定の場合は設定ファイルまたは環境変数から取得します。

        設定の読み込み順序（後のものが優先）:
            1. /content/drive/MyDrive/drive_ws/secret/jquants-api.toml (Google Colab のみ)
            2. ${HOME}/.jquants-api/jquants-api.toml
            3. jquants-api.toml (カレントディレクトリ)
            4. os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"] で指定されたファイル
            5. 環境変数 JQUANTS_API_KEY
        """
        config = self._load_config()

        if api_key is not None:
            self._api_key = api_key
        else:
            self._api_key = config.get("api_key", "")

        if not self._api_key:
            raise ValueError(
                "api_key is required. Set it via argument, config file, or JQUANTS_API_KEY env var."
            )

        self._session: Optional[requests.Session] = None

        # API 実装 (v2)
        self._eq_master_api = EqMasterApiV2()
        self._eq_bars_daily_api = EqBarsDailyApiV2()
        self._eq_bars_daily_am_api = EqBarsDailyAmApiV2()
        self._eq_bars_minute_api = EqBarsMinuteApiV2()
        self._eq_investor_types_api = EqInvestorTypesApiV2()
        self._fin_summary_api = FinSummaryApiV2()
        self._fin_details_api = FinDetailsApiV2()
        self._fin_dividend_api = FinDividendApiV2()
        self._eq_earnings_cal_api = EqEarningsCalApiV2()
        self._mkt_short_ratio_api = MktShortRatioApiV2()
        self._mkt_margin_interest_api = MktMarginInterestApiV2()
        self._mkt_breakdown_api = MktBreakdownApiV2()
        self._mkt_calendar_api = MktCalendarApiV2()
        self._mkt_short_sale_report_api = MktShortSaleReportApiV2()
        self._mkt_margin_alert_api = MktMarginAlertApiV2()
        self._idx_bars_daily_api = IdxBarsDailyApiV2()
        self._idx_bars_daily_topix_api = IdxBarsDailyTopixApiV2()
        self._drv_bars_daily_fut_api = DrvBarsDailyFutApiV2()
        self._drv_bars_daily_opt_api = DrvBarsDailyOptApiV2()
        self._drv_bars_daily_opt_225_api = DrvBarsDailyOpt225ApiV2()
        self._bulk_list_api = BulkListApiV2()
        self._bulk_get_api = BulkGetApiV2()

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _is_colab(self) -> bool:
        """
        Return True if running in colab
        """
        return "google.colab" in sys.modules

    def _load_config(self) -> dict:
        """
        load config from files and environment variables

        Args:
            N/A
        Returns:
            dict: configurations
        """
        config: dict = {}

        # colab config
        if self._is_colab():
            colab_config_path = (
                "/content/drive/MyDrive/drive_ws/secret/jquants-api.toml"
            )
            config = {**config, **self._read_config(colab_config_path)}

        # user default config
        user_config_path = f"{Path.home()}/.jquants-api/jquants-api.toml"
        config = {**config, **self._read_config(user_config_path)}

        # current dir config
        current_config_path = "jquants-api.toml"
        config = {**config, **self._read_config(current_config_path)}

        # env specified config
        if "JQUANTS_API_CLIENT_CONFIG_FILE" in os.environ:
            env_config_path = os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]
            config = {**config, **self._read_config(env_config_path)}

        # env var (highest priority)
        config["api_key"] = os.environ.get("JQUANTS_API_KEY", config.get("api_key", ""))

        return config

    def _read_config(self, config_path: str) -> dict:
        """
        read config from a toml file

        Params:
            config_path: a path to a toml file
        """
        if not os.path.isfile(config_path):
            return {}

        with open(config_path, mode="rb") as f:
            ret = tomllib.load(f)

        if "jquants-api-client" not in ret:
            return {}

        return ret["jquants-api-client"]

    def _request_session(
        self,
        status_forcelist: Optional[list[int]] = None,
        allowed_methods: Optional[list[str]] = None,
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
                pool_connections=self.MAX_WORKERS + 10,
                pool_maxsize=self.MAX_WORKERS + 10,
                max_retries=retry_strategy,
            )
            self._session = requests.Session()
            self._session.mount("https://", adapter)

        return self._session

    def _base_headers(self) -> dict[str, str]:
        """
        J-Quants API v2 にアクセスする際の共通ヘッダを生成
        """
        return {
            "x-api-key": self._api_key,
            "User-Agent": f"{self.USER_AGENT}/{self.USER_AGENT_VERSION} "
            f"p/{platform.python_version()}",
        }

    def _get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> requests.Response:
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
        params: Optional[dict[str, Any]] = None,
        data_key: str = "data",
    ) -> list[dict[str, Any]]:
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
        all_data: list[dict[str, Any]] = []
        query: dict[str, Any] = dict(params or {})

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
    # eq-master (/equities/master)
    # ------------------------------------------------------------------
    def get_eq_master(
        self,
        code: str = "",
        date: str = "",
    ) -> pd.DataFrame:
        """
        eq-master: 上場銘柄一覧 (v2: /equities/master)

        Args:
            code: 5桁の銘柄コード (例: 27800)。4桁指定も可能。
            date: 基準日 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 上場銘柄情報
        """
        return self._eq_master_api.execute(self, code=code, date=date)

    # ------------------------------------------------------------------
    # ユーティリティ: 業種・市場区分マスタ (v1 と同様のローカル定義)
    # ------------------------------------------------------------------
    def get_market_segments(self) -> pd.DataFrame:
        """
        市場区分コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(
            constants.MARKET_SEGMENT_DATA, columns=constants.MARKET_SEGMENT_COLUMNS_V2
        )
        df.sort_values(constants.MARKET_SEGMENT_COLUMNS_V2[0], inplace=True)
        return df

    def get_17_sectors(self) -> pd.DataFrame:
        """
        17 業種コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(
            constants.SECTOR_17_DATA, columns=constants.SECTOR_17_COLUMNS_V2
        )
        df.sort_values(constants.SECTOR_17_COLUMNS_V2[0], inplace=True)
        return df

    def get_33_sectors(self) -> pd.DataFrame:
        """
        33 業種コードと名称 (V2 カラム名)
        """
        df = pd.DataFrame(
            constants.SECTOR_33_DATA, columns=constants.SECTOR_33_COLUMNS_V2
        )
        df.sort_values(constants.SECTOR_33_COLUMNS_V2[0], inplace=True)
        return df

    # ------------------------------------------------------------------
    # get_list (v1 と同名のユーティリティ, eq-master ベース)
    # ------------------------------------------------------------------
    def get_list(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        上場銘柄一覧 (業種・市場区分の英語名を付与したユーティリティ)

        v2 の eq-master を利用し、v2 フィールド名で返却します。

        Args:
            code: 銘柄コード (任意)
            date_yyyymmdd: 基準日 (YYYYMMDD or YYYY-MM-DD, 任意)
        Returns:
            pd.DataFrame: 上場銘柄情報 (v2 フィールド名)
        """
        df_list = self.get_eq_master(code=code, date=date_yyyymmdd)
        if df_list.empty:
            return pd.DataFrame([], columns=constants.EQ_MASTER_COLUMNS_V2)

        # 17/33 業種 & 市場区分の英語名を付与
        df_17_sectors = self.get_17_sectors()[["S17", "S17NmEn"]]
        df_33_sectors = self.get_33_sectors()[["S33", "S33NmEn"]]
        df_segments = self.get_market_segments()[["Mkt", "MktNmEn"]]

        df_list = pd.merge(df_list, df_17_sectors, how="left", on=["S17"])
        df_list = pd.merge(df_list, df_33_sectors, how="left", on=["S33"])
        df_list = pd.merge(df_list, df_segments, how="left", on=["Mkt"])

        df_list.sort_values("Code", inplace=True)
        return df_list

    # ------------------------------------------------------------------
    # eq-bars-daily (/equities/bars/daily)
    # ------------------------------------------------------------------
    def get_eq_bars_daily(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-daily: 株価四本値 (v2: /equities/bars/daily)

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 株価データ (v2のフィールド名で返却)
        """
        return self._eq_bars_daily_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_eq_bars_daily_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全銘柄の株価四本値を日付範囲指定して取得 (v2: /equities/bars/daily)

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
        Returns:
            pd.DataFrame: 株価データ (Code, Date 列でソート)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_eq_bars_daily, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # eq-bars-daily-am (/equities/bars/daily/am)
    # ------------------------------------------------------------------
    def get_eq_bars_daily_am(self, code: str = "") -> pd.DataFrame:
        """
        eq-bars-daily-am: 前場四本値 (v2: /equities/bars/daily/am)

        Args:
            code: 銘柄コード (5桁 or 4桁)。空文字の場合は全銘柄。
        Returns:
            pd.DataFrame: 前場の株価データ
        """
        return self._eq_bars_daily_am_api.execute(self, code=code)

    # ------------------------------------------------------------------
    # eq-bars-minute (/equities/bars/minute)
    # ------------------------------------------------------------------
    def get_eq_bars_minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute: 分足 (v2: /equities/bars/minute)

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 1分足データ (v2のフィールド名で返却)
        """
        return self._eq_bars_minute_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def _aggregate_bars_n_minute(
        self,
        df: pd.DataFrame,
        n: int = 5,
    ) -> pd.DataFrame:
        """
        1分足データをn分足に集約する (private)

        Args:
            df: 1分足データ (Date, Time, Code, O, H, L, C, Vo, Va を含む)
            n: 集約する分数 (デフォルト: 5)
        Returns:
            pd.DataFrame: n分足データ
        """
        if df.empty:
            return df.copy()

        df = df.copy()

        # DateTime列を作成 (Date + Time)
        df["DateTime"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str),
            errors="coerce",
        )

        # n分間隔でグループ化するためのキーを作成
        df["TimeGroup"] = df["DateTime"].dt.floor(f"{n}min")

        # 銘柄ごと・n分間隔ごとに集約
        agg_funcs = {
            "Date": "first",
            "Time": "first",
            "Code": "first",
            "O": "first",  # 始値: 最初の値
            "H": "max",  # 高値: 最大値
            "L": "min",  # 安値: 最小値
            "C": "last",  # 終値: 最後の値
            "Vo": "sum",  # 出来高: 合計
            "Va": "sum",  # 売買代金: 合計
        }

        result = (
            df.groupby(["Code", "TimeGroup"], as_index=False)
            .agg(agg_funcs)
            .drop(columns=["TimeGroup"])
        )

        # ソートして返却
        result.sort_values(["Code", "Date", "Time"], inplace=True)
        return result.reset_index(drop=True)

    def get_eq_bars_5minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute から5分足データを算出して取得

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 5分足データ
        """
        df_1min = self.get_eq_bars_minute(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        return self._aggregate_bars_n_minute(df_1min, n=5)

    def get_eq_bars_15minute(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-bars-minute から15分足データを算出して取得

        Args:
            code: 銘柄コード (5桁 or 4桁)
            from_yyyymmdd: 期間開始日 (YYYYMMDD or YYYY-MM-DD)
            to_yyyymmdd: 期間終了日 (YYYYMMDD or YYYY-MM-DD)
            date_yyyymmdd: 特定日付 (YYYYMMDD or YYYY-MM-DD)
        Returns:
            pd.DataFrame: 15分足データ
        """
        df_1min = self.get_eq_bars_minute(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        return self._aggregate_bars_n_minute(df_1min, n=15)

    # ------------------------------------------------------------------
    # eq-investor-types (/equities/investor-types)
    # ------------------------------------------------------------------
    def get_eq_investor_types(
        self,
        section: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        eq-investor-types: 投資部門別売買状況 (v2: /equities/investor-types)

        Args:
            section: 市場区分 (例: \"TSEPrime\")
            from_yyyymmdd: 期間開始日
            to_yyyymmdd: 期間終了日
        Returns:
            pd.DataFrame: 投資部門別売買データ
        """
        return self._eq_investor_types_api.execute(
            self,
            section=section,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    # ------------------------------------------------------------------
    # /fins/summary (path_old: /fins/statements)
    # ------------------------------------------------------------------
    def get_fin_summary(
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
        return self._fin_summary_api.execute(
            self,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_fin_summary_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike = datetime.now(),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報サマリを日付範囲指定して取得 (v2: /fins/summary)

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ (未指定時はキャッシュしない)
        """
        buff: list[pd.DataFrame] = []
        futures: dict[Any, str] = {}
        dates = pd.date_range(start_dt, end_dt, freq="D")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for s in dates:
                yyyymmdd = s.strftime("%Y%m%d")
                yyyy = yyyymmdd[:4]
                cache_file = f"v2_fin_summary_{yyyymmdd}.csv.gz"

                if (cache_dir != "") and os.path.isfile(
                    f"{cache_dir}/{yyyy}/{cache_file}"
                ):
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}", dtype=str)
                    # 日付カラムをdatetimeに変換
                    date_cols = [
                        "DiscDate",
                        "CurPerSt",
                        "CurPerEn",
                        "CurFYSt",
                        "CurFYEn",
                        "NxtFYSt",
                        "NxtFYEn",
                    ]
                    for col in date_cols:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors="coerce")
                    buff.append(df)
                else:
                    future = executor.submit(
                        self.get_fin_summary, date_yyyymmdd=yyyymmdd
                    )
                    futures[future] = yyyymmdd

            for future in as_completed(futures):
                df = future.result()
                if df.empty:
                    continue
                buff.append(df)
                yyyymmdd = futures[future]
                yyyy = yyyymmdd[:4]
                cache_file = f"v2_fin_summary_{yyyymmdd}.csv.gz"
                if cache_dir != "":
                    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
                    df.to_csv(f"{cache_dir}/{yyyy}/{cache_file}", index=False)

        if not buff:
            return pd.DataFrame()

        return (
            pd.concat(buff)
            .sort_values(["DiscDate", "DiscTime", "Code"])
            .reset_index(drop=True)
        )

    # ------------------------------------------------------------------
    # /fins/details (path_old: /fins/fs_details)
    # ------------------------------------------------------------------
    def get_fin_details(
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
        return self._fin_details_api.execute(
            self,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_fin_details_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike = datetime.now(),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務諸表詳細を日付範囲指定して取得 (v2: /fins/details)

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ (未指定時はキャッシュしない)
        """
        buff: list[pd.DataFrame] = []
        futures: dict[Any, str] = {}
        dates = pd.date_range(start_dt, end_dt, freq="D")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for s in dates:
                yyyymmdd = s.strftime("%Y%m%d")
                yyyy = yyyymmdd[:4]
                cache_file = f"v2_fin_details_{yyyymmdd}.csv.gz"

                if (cache_dir != "") and os.path.isfile(
                    f"{cache_dir}/{yyyy}/{cache_file}"
                ):
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}", dtype=str)
                    if "DiscDate" in df.columns:
                        df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")
                    buff.append(df)
                else:
                    future = executor.submit(
                        self.get_fin_details, date_yyyymmdd=yyyymmdd
                    )
                    futures[future] = yyyymmdd

            for future in as_completed(futures):
                df = future.result()
                if df.empty:
                    continue
                buff.append(df)
                yyyymmdd = futures[future]
                yyyy = yyyymmdd[:4]
                cache_file = f"v2_fin_details_{yyyymmdd}.csv.gz"
                if cache_dir != "":
                    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
                    df.to_csv(f"{cache_dir}/{yyyy}/{cache_file}", index=False)

        if not buff:
            return pd.DataFrame()

        return (
            pd.concat(buff)
            .sort_values(["DiscDate", "DiscTime", "Code"])
            .reset_index(drop=True)
        )

    # ------------------------------------------------------------------
    # /fins/dividend (path_old: /fins/dividend)
    # ------------------------------------------------------------------
    def get_fin_dividend(
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
        return self._fin_dividend_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    # ------------------------------------------------------------------
    # /equities/earnings-calendar (path_old: /fins/announcement)
    # ------------------------------------------------------------------
    def get_eq_earnings_cal(self) -> pd.DataFrame:
        """
        決算発表予定日 (v2: /equities/earnings-calendar)

        Returns:
            pd.DataFrame: 決算発表予定データ
        """
        return self._eq_earnings_cal_api.execute(self)

    # ------------------------------------------------------------------
    # /markets/short-ratio (path_old: /markets/short_selling)
    # ------------------------------------------------------------------
    def get_mkt_short_ratio(
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
        return self._mkt_short_ratio_api.execute(
            self,
            sector_33_code=sector_33_code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_mkt_short_ratio_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全33業種の空売り比率データを日付範囲指定して取得 (v2: /markets/short-ratio)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_mkt_short_ratio, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Date", "S33"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/short-sale-report (path_old: /markets/short_selling_positions)
    # ------------------------------------------------------------------
    def get_mkt_short_sale_report(
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
        return self._mkt_short_sale_report_api.execute(
            self,
            code=code,
            disclosed_date=disclosed_date,
            disclosed_date_from=disclosed_date_from,
            disclosed_date_to=disclosed_date_to,
            calculated_date=calculated_date,
        )

    def get_mkt_short_sale_report_range(
        self,
        start_dt: DatetimeLike = "20131107",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        空売り残高報告データを日付範囲指定して取得 (v2: /markets/short-sale-report)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_mkt_short_sale_report,
                    disclosed_date=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return (
            pd.concat(buff)
            .sort_values(["DiscDate", "CalcDate", "Code"])
            .reset_index(drop=True)
        )

    # ------------------------------------------------------------------
    # /markets/margin-interest (path_old: /markets/weekly_margin_interest)
    # ------------------------------------------------------------------
    def get_mkt_margin_interest(
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
        return self._mkt_margin_interest_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_mkt_margin_interest_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        信用取引週末残高を日付範囲指定して取得 (v2: /markets/margin-interest)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_mkt_margin_interest,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Date", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/margin-alert (path_old: /markets/daily_margin_interest)
    # ------------------------------------------------------------------
    def get_mkt_margin_alert(
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
        return self._mkt_margin_alert_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_mkt_margin_alert_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        日々公表信用取引残高を日付範囲指定して取得 (v2: /markets/margin-alert)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_mkt_margin_alert,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["PubDate", "Code"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/breakdown (path_old: /markets/breakdown)
    # ------------------------------------------------------------------
    def get_mkt_breakdown(
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
        return self._mkt_breakdown_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_mkt_breakdown_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        売買内訳データを日付範囲指定して取得 (v2: /markets/breakdown)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_mkt_breakdown, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # /markets/calendar (path_old: /markets/trading_calendar)
    # ------------------------------------------------------------------
    def get_mkt_calendar(
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
        return self._mkt_calendar_api.execute(
            self,
            holiday_division=holiday_division,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    # ------------------------------------------------------------------
    # indices (v2: /indices/bars/daily, /indices/bars/daily/topix)
    # ------------------------------------------------------------------
    def get_idx_bars_daily(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        指数四本値 (v2: /indices/bars/daily)

        Args:
            code: 指数コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
        """
        return self._idx_bars_daily_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_idx_bars_daily_topix(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        TOPIX 指数四本値 (v2: /indices/bars/daily/topix)

        Args:
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
        """
        return self._idx_bars_daily_topix_api.execute(
            self,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    # ------------------------------------------------------------------
    # derivatives (v2: /derivatives/bars/daily/*)
    # ------------------------------------------------------------------
    def get_drv_bars_daily_fut(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        先物四本値 (v2: /derivatives/bars/daily/futures)
        """
        return self._drv_bars_daily_fut_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
            category=category,
            contract_flag=contract_flag,
        )

    def get_drv_bars_daily_opt(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        オプション四本値 (v2: /derivatives/bars/daily/options)
        """
        return self._drv_bars_daily_opt_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
            category=category,
            contract_flag=contract_flag,
            code=code,
        )

    def get_drv_bars_daily_opt_225(
        self,
        date_yyyymmdd: str,
    ) -> pd.DataFrame:
        """
        日経225オプション四本値 (v2: /derivatives/bars/daily/options/225)
        """
        return self._drv_bars_daily_opt_225_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_drv_bars_daily_fut_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        先物四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/futures)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_drv_bars_daily_fut,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    def get_drv_bars_daily_opt_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        オプション四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/options)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            options = [
                executor.submit(
                    self.get_drv_bars_daily_opt,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                    code=code,
                )
                for s in dates
            ]
            for option in as_completed(options):
                df = option.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    def get_drv_bars_daily_opt_225_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        日経225オプション四本値を日付範囲指定して取得 (v2: /derivatives/bars/daily/options/225)
        """
        buff: list[pd.DataFrame] = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_drv_bars_daily_opt_225,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                if not df.empty:
                    buff.append(df)
        if not buff:
            return pd.DataFrame()
        return pd.concat(buff).sort_values(["Code", "Date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # Bulk API
    # ------------------------------------------------------------------
    def get_bulk_list(
        self,
        endpoint: Union[str, BulkEndpoint],
    ) -> pd.DataFrame:
        """
        bulk-list: 取得可能なデータ一覧 (v2: /bulk/list)

        Args:
            endpoint: 取得したいデータのエンドポイント
                      (例: BulkEndpoint.EQ_MASTER または "/equities/master")
        Returns:
            pd.DataFrame: データ一覧 (Key, Size, LastModified)
        """
        return self._bulk_list_api.execute(self, endpoint=endpoint)

    def get_bulk(self, key: str) -> str:
        """
        bulk-get: データダウンロードURL取得 (v2: /bulk/get)

        Args:
            key: get_bulk_listで取得したKey
        Returns:
            str: ダウンロードURL
        """
        return self._bulk_get_api.execute(self, key=key)

    def download_bulk(self, key: str, output_path: str) -> None:
        """
        bulk-get で取得した URL からファイルをダウンロードして保存

        Args:
            key: get_bulk_listで取得したKey
            output_path: ダウンロードファイルの保存先パス

        Raises:
            ValueError: output_path が空文字列の場合
        """
        # バリデーション
        if not output_path or not output_path.strip():
            raise ValueError("output_path must not be empty")

        # ダウンロード URL を取得
        url = self._bulk_get_api.execute(self, key=key)

        # ディレクトリが存在しない場合は作成
        output_dir = os.path.dirname(os.path.abspath(output_path))
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # ファイルをダウンロード
        session = self._request_session()
        response = session.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # ファイルに書き込み
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
