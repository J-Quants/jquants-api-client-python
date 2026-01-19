import os
import platform
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Union

import pandas as pd  # type: ignore
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util import Retry

from jquantsapi import __version__, constants, enums
from jquantsapi.apis.v1.derivatives import (
    DerivativesFuturesApiV1,
    DerivativesOptionsApiV1,
    OptionIndexOptionApiV1,
)
from jquantsapi.apis.v1.fins import (
    FinsAnnouncementApiV1,
    FinsDividendApiV1,
    FinsFsDetailsApiV1,
    FinsStatementsApiV1,
)
from jquantsapi.apis.v1.indices import IndicesApiV1, IndicesTopixApiV1
from jquantsapi.apis.v1.listed import ListedInfoApiV1
from jquantsapi.apis.v1.markets import (
    MarketsBreakdownApiV1,
    MarketsDailyMarginInterestApiV1,
    MarketsShortSellingApiV1,
    MarketsShortSellingPositionsApiV1,
    MarketsTradesSpecApiV1,
    MarketsTradingCalendarApiV1,
    MarketsWeeklyMarginInterestApiV1,
)
from jquantsapi.apis.v1.prices import PricesDailyQuotesApiV1, PricesPricesAmApiV1

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

if sys.version_info >= (3, 13):
    from warnings import deprecated  # type: ignore[attr-defined]
else:
    from typing_extensions import deprecated


DatetimeLike = Union[datetime, pd.Timestamp, str]
_Data = Union[str, Mapping[str, Any]]


class TokenAuthRefreshBadRequestException(Exception):
    pass


@deprecated(
    "Client (V1) is deprecated and will be removed in a future version. Please use ClientV2 instead."
)
class Client:
    """
    J-Quants API からデータを取得する
    ref. https://jpx.gitbook.io/j-quants-ja/

    .. deprecated::
        This class is deprecated. Use :class:`ClientV2` instead.
    """

    JQUANTS_API_BASE = "https://api.jquants.com/v1"
    MAX_WORKERS = 5
    USER_AGENT = "jqapi-python"
    USER_AGENT_VERSION = __version__
    RAW_ENCODING = "utf-8"

    def __init__(
        self,
        refresh_token: Optional[str] = None,
        *,
        mail_address: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        Args:
            refresh_token: J-Quants API refresh token
            mail_address: J-Quants API login email address
            password: J-Quants API login password
        """
        config = self._load_config()

        self._mail_address = config["mail_address"]
        if mail_address is not None:
            self._mail_address = mail_address

        self._password = config["password"]
        if password is not None:
            self._password = password

        self._refresh_token = config["refresh_token"]
        if refresh_token is not None:
            self._refresh_token = refresh_token

        if self._refresh_token != "":
            self._refresh_token_expire = pd.Timestamp.utcnow() + pd.Timedelta(
                6, unit="D"
            )
        else:
            self._refresh_token_expire = pd.Timestamp.utcnow()

        self._id_token = ""
        self._id_token_expire = pd.Timestamp.utcnow()
        self._session: Optional[requests.Session] = None

        # API 実装 (v1)
        self._listed_info_api = ListedInfoApiV1()
        self._prices_daily_quotes_api = PricesDailyQuotesApiV1()
        self._prices_prices_am_api = PricesPricesAmApiV1()
        self._markets_trades_spec_api = MarketsTradesSpecApiV1()
        self._markets_weekly_margin_interest_api = MarketsWeeklyMarginInterestApiV1()
        self._indices_api = IndicesApiV1()
        self._indices_topix_api = IndicesTopixApiV1()
        self._derivatives_futures_api = DerivativesFuturesApiV1()
        self._derivatives_options_api = DerivativesOptionsApiV1()
        self._option_index_option_api = OptionIndexOptionApiV1()
        self._markets_weekly_margin_interest_api = MarketsWeeklyMarginInterestApiV1()
        self._markets_short_selling_api = MarketsShortSellingApiV1()
        self._markets_breakdown_api = MarketsBreakdownApiV1()
        self._markets_short_selling_positions_api = MarketsShortSellingPositionsApiV1()
        self._markets_daily_margin_interest_api = MarketsDailyMarginInterestApiV1()
        self._markets_trading_calendar_api = MarketsTradingCalendarApiV1()
        self._fins_statements_api = FinsStatementsApiV1()
        self._fins_fs_details_api = FinsFsDetailsApiV1()
        self._fins_dividend_api = FinsDividendApiV1()
        self._fins_announcement_api = FinsAnnouncementApiV1()

        if ((self._mail_address == "") or (self._password == "")) and (
            self._refresh_token == ""
        ):
            raise ValueError(
                "Either mail_address/password or refresh_token is required."
            )
        if (self._mail_address != "") and ("@" not in self._mail_address):
            raise ValueError("mail_address must contain '@' character.")

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

        # env vars
        config["mail_address"] = os.environ.get(
            "JQUANTS_API_MAIL_ADDRESS", config.get("mail_address", "")
        )
        config["password"] = os.environ.get(
            "JQUANTS_API_PASSWORD", config.get("password", "")
        )
        config["refresh_token"] = os.environ.get(
            "JQUANTS_API_REFRESH_TOKEN", config.get("refresh_token", "")
        )

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

    def _base_headers(self) -> dict:
        """
        J-Quants API にアクセスする際にヘッダーにIDトークンを設定
        """
        id_token = self.get_id_token()
        headers = {
            "Authorization": f"Bearer {id_token}",
            "User-Agent": f"{self.USER_AGENT}/{self.USER_AGENT_VERSION} p/{platform.python_version()}",
        }
        return headers

    def _request_session(
        self,
        status_forcelist: Optional[list[int]] = None,
        allowed_methods: Optional[list[str]] = None,
    ) -> requests.Session:
        """
        requests の session 取得

        リトライを設定

        Args:
            status_forcelist: リトライ対象のステータスコード
            allowed_methods: リトライ対象のメソッド
        Returns:
            requests.session
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
        if allowed_methods is None:
            allowed_methods = ["HEAD", "GET", "OPTIONS", "POST"]

        if self._session is None:
            retry_strategy = Retry(
                total=3,
                status_forcelist=status_forcelist,
                allowed_methods=allowed_methods,
            )
            adapter = HTTPAdapter(
                # 安全のため並列スレッド数に更に10追加しておく
                pool_connections=self.MAX_WORKERS + 10,
                pool_maxsize=self.MAX_WORKERS + 10,
                max_retries=retry_strategy,
            )
            self._session = requests.Session()
            self._session.mount("https://", adapter)

        return self._session

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """
        requests の get 用ラッパー

        ヘッダーにアクセストークンを設定
        タイムアウトを設定

        Args:
            url: アクセスするURL
            params: パラメーター

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        headers = self._base_headers()
        ret = s.get(url, params=params, headers=headers, timeout=30)
        if ret.status_code == 400:
            msg = f"{ret.status_code} for url: {ret.url} body: {ret.text}"
            raise HTTPError(msg, response=ret)
        ret.raise_for_status()
        return ret

    def _post(
        self,
        url: str,
        data: Optional[_Data] = None,
        json: Optional[Any] = None,
        headers: Optional[dict] = None,
    ) -> requests.Response:
        """
        requests の post 用ラッパー

        タイムアウトを設定

        Args:
            url: アクセスするURL
            payload: 送信するデータ
            headers: HTTPヘッダ

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        base_headers = {
            "User-Agent": f"{self.USER_AGENT}/{self.USER_AGENT_VERSION} p/{platform.python_version()}",
        }
        if headers is not None:
            base_headers.update(headers)

        ret = s.post(url, data=data, json=json, headers=base_headers, timeout=30)
        ret.raise_for_status()
        return ret

    # /token
    def get_refresh_token(
        self, mail_address: Optional[str] = None, password: Optional[str] = None
    ) -> str:
        """
        get J-Quants API refresh token

        Params:
            mail_address: J-Quants API login email address
            password: J-Quants API login password
        Returns:
            refresh_token: J-Quants API refresh token
        """
        if self._refresh_token_expire > pd.Timestamp.utcnow():
            return self._refresh_token

        if mail_address is None:
            mail_address = self._mail_address
        if password is None:
            password = self._password

        if mail_address == "" or password == "":
            raise ValueError("mail_address/password are required")
        if (mail_address is not None) and ("@" not in mail_address):
            raise ValueError("mail_address must contain '@' character.")

        url = f"{self.JQUANTS_API_BASE}/token/auth_user"
        data = {
            "mailaddress": mail_address,
            "password": password,
        }
        ret = self._post(url, json=data)
        refresh_token = ret.json()["refreshToken"]
        self._refresh_token = refresh_token
        self._refresh_token_expire = pd.Timestamp.utcnow() + pd.Timedelta(6, unit="D")
        return self._refresh_token

    @retry(
        retry=retry_if_exception_type(TokenAuthRefreshBadRequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=300),
    )
    def get_id_token(self, refresh_token: Optional[str] = None) -> str:
        """
        get J-Quants API id_token

        Params:
            refresh_token: J-Quants API refresh token
        Returns:
            id_token: J-Quants API id token
        """
        if self._id_token_expire > pd.Timestamp.utcnow():
            return self._id_token

        if refresh_token is not None:
            _refresh_token = refresh_token
        else:
            _refresh_token = self.get_refresh_token()

        url = (
            f"{self.JQUANTS_API_BASE}/token/auth_refresh?refreshtoken={_refresh_token}"
        )
        try:
            ret = self._post(url)
        except HTTPError as e:
            # retry if:
            # - refresh_token is not provided as a parameter
            # - error is 400 bad request (refresh_token expire)
            # - mail_address and password are provided
            if (
                refresh_token is None
                and e.response.status_code == 400
                and self._mail_address != ""
                and self._password != ""
            ):
                # clear tokens for the next try
                self._refresh_token = ""
                self._refresh_token_expire = pd.Timestamp.utcnow()
                self._id_token = ""
                self._id_token_expire = pd.Timestamp.utcnow()
                # raise for retrying
                raise TokenAuthRefreshBadRequestException(e)
            raise e
        id_token = ret.json()["idToken"]
        self._id_token = id_token
        self._id_token_expire = pd.Timestamp.utcnow() + pd.Timedelta(23, unit="hour")
        return self._id_token

    def get_listed_info(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        Get listed companies

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            pd.DataFrame: listed companies (sorted by Code)
        """
        return self._listed_info_api.execute(
            self,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )

    @staticmethod
    def get_market_segments() -> pd.DataFrame:
        """
        Get market segment code and name

        Args:
            N/A

        Returns:
            pd.DataFrame: market segment code and name

        """

        df = pd.DataFrame(
            constants.MARKET_SEGMENT_DATA, columns=constants.MARKET_SEGMENT_COLUMNS
        )
        df.sort_values(constants.MARKET_SEGMENT_COLUMNS[0], inplace=True)
        return df

    def get_17_sectors(self) -> pd.DataFrame:
        """
        Get 17-sector code and name
        ref. https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/17-sector

        Args:
            N/A

        Returns:
            pd.DataFrame: 17-sector code and name
        """
        df = pd.DataFrame(constants.SECTOR_17_DATA, columns=constants.SECTOR_17_COLUMNS)
        df.sort_values(constants.SECTOR_17_COLUMNS[0], inplace=True)
        return df

    def get_33_sectors(self) -> pd.DataFrame:
        """
        Get 33-sector code and name
        ref. https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/33-sector

        Args:
            N/A

        Returns:
            pd.DataFrame: 33-sector code and name
        """
        df = pd.DataFrame(constants.SECTOR_33_DATA, columns=constants.SECTOR_33_COLUMNS)
        df.sort_values(constants.SECTOR_33_COLUMNS[0], inplace=True)
        return df

    def get_list(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        Get listed companies (incl English name for sectors/segments)

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            pd.DataFrame: listed companies
        """
        df_list = self.get_listed_info(code=code, date_yyyymmdd=date_yyyymmdd)
        df_17_sectors = self.get_17_sectors()[
            ["Sector17Code", "Sector17CodeNameEnglish"]
        ]
        df_33_sectors = self.get_33_sectors()[
            ["Sector33Code", "Sector33CodeNameEnglish"]
        ]
        df_segments = self.get_market_segments()[
            ["MarketCode", "MarketCodeNameEnglish"]
        ]
        df_list = pd.merge(df_list, df_17_sectors, how="left", on=["Sector17Code"])
        df_list = pd.merge(df_list, df_33_sectors, how="left", on=["Sector33Code"])
        df_list = pd.merge(df_list, df_segments, how="left", on=["MarketCode"])
        df_list.sort_values("Code", inplace=True)
        return df_list

    def get_prices_daily_quotes(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        株価情報を取得

        Args:
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日

        Returns:
            pd.DataFrame: 株価情報 (Code, Date列でソートされています)
        """
        return self._prices_daily_quotes_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_price_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全銘柄の株価情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 株価情報 (Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_prices_daily_quotes, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_prices_prices_am(
        self,
        code: str = "",
    ) -> pd.DataFrame:
        """
        get the morning session's high, low, opening, and closing prices for individual stocks API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
        Returns: pd.DataFrame: the morning session's OHLC data
        """
        return self._prices_prices_am_api.execute(self, code=code)

    # /markets
    def get_markets_trades_spec(
        self,
        section: Union[str, enums.MARKET_API_SECTIONS] = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        Weekly Trading by Type of Investors

        Args:
            section: section name (e.g. "TSEPrime" or MARKET_API_SECTIONS.TSEPrime)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: Weekly Trading by Type of Investors (Sorted by "PublishedDate" and "Section" columns)
        """
        return self._markets_trades_spec_api.execute(
            self,
            section=section,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    def get_markets_weekly_margin_interest(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        get weekly margin interest API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: weekly margin interest (Sorted by "Date" and "Code" columns)
        """
        return self._markets_weekly_margin_interest_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_weekly_margin_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        信用取引週末残高を日付範囲を指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 信用取引週末残高(Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_markets_weekly_margin_interest,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_markets_short_selling(
        self,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        get daily short sale ratios and trading value by industry (sector) API returns

        Args:
            sector_33_code: 33-sector code (e.g. 0050 or 8050)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame:
                daily short sale ratios and trading value by industry (Sorted by "Date" and "Sector33Code" columns)
        """
        return self._markets_short_selling_api.execute(
            self,
            sector_33_code=sector_33_code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_short_selling_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全３３業種の空売り比率に関する売買代金を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 空売り比率に関する売買代金 (Sector33Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_markets_short_selling, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Sector33Code", "Date"])

    def get_markets_breakdown(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        get detail breakdown trading data API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: detail breakdown trading data (Sorted by "Code")
        """
        return self._markets_breakdown_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_breakdown_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        売買内訳データを日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 売買内訳データ(Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_markets_breakdown, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    # /indices

    def get_indices(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        Indices Daily OHLC

        Args:
            code: 指数コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
        Returns:
            pd.DataFrame: Indices Daily OHLC (Sorted by "Code", "Date" column)
        """
        return self._indices_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_indices_topix(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        TOPIX Daily OHLC

        Args:
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: TOPIX Daily OHLC (Sorted by "Date" column)
        """
        return self._indices_topix_api.execute(
            self,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    def get_fins_statements(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務情報取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務情報 (DisclosedDate, DisclosedTime, 及びLocalCode列でソートされています)
        """
        return self._fins_statements_api.execute(
            self,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_statements_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike = datetime.now(),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ

        Returns:
            pd.DataFrame: 財務情報 (DisclosedDate, DisclosedTime, 及びLocalCode列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()

        buff = []
        futures = {}
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for s in dates:
                # fetch data via API or cache file
                yyyymmdd = s.strftime("%Y%m%d")
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_statements_{yyyymmdd}.csv.gz"
                if (cache_dir != "") and os.path.isfile(
                    f"{cache_dir}/{yyyy}/{cache_file}"
                ):
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}", dtype=str)
                    df["DisclosedDate"] = pd.to_datetime(
                        df["DisclosedDate"], format="%Y-%m-%d"
                    )
                    df["CurrentPeriodStartDate"] = pd.to_datetime(
                        df["CurrentPeriodStartDate"], format="%Y-%m-%d"
                    )
                    df["CurrentPeriodEndDate"] = pd.to_datetime(
                        df["CurrentPeriodEndDate"], format="%Y-%m-%d"
                    )
                    df["CurrentFiscalYearStartDate"] = pd.to_datetime(
                        df["CurrentFiscalYearStartDate"], format="%Y-%m-%d"
                    )
                    df["CurrentFiscalYearEndDate"] = pd.to_datetime(
                        df["CurrentFiscalYearEndDate"], format="%Y-%m-%d"
                    )
                    df["NextFiscalYearStartDate"] = pd.to_datetime(
                        df["NextFiscalYearStartDate"], format="%Y-%m-%d"
                    )
                    df["NextFiscalYearEndDate"] = pd.to_datetime(
                        df["NextFiscalYearEndDate"], format="%Y-%m-%d"
                    )
                    buff.append(df)
                else:
                    future = executor.submit(
                        self.get_fins_statements, date_yyyymmdd=yyyymmdd
                    )
                    futures[future] = yyyymmdd
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
                yyyymmdd = futures[future]
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_statements_{yyyymmdd}.csv.gz"
                if cache_dir != "":
                    # create year directory
                    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
                    # write cache file
                    df.to_csv(f"{cache_dir}/{yyyy}/{cache_file}", index=False)

        return pd.concat(buff).sort_values(
            ["DisclosedDate", "DisclosedTime", "LocalCode"]
        )

    def get_fins_fs_details(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務諸表(BS/PL)取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務諸表(BS/PL) (DisclosedDate, DisclosedTime, 及びLocalCode列でソートされています)
        """
        return self._fins_fs_details_api.execute(
            self,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_fs_details_range(
        self,
        start_dt: DatetimeLike = "20080707",
        end_dt: DatetimeLike = datetime.now(),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務諸表(BS/PL)を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ

        Returns:
            pd.DataFrame: 財務諸表(BS/PL) (DisclosedDate, DisclosedTime, 及びLocalCode列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()

        buff = []
        futures = {}
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for s in dates:
                # fetch data via API or cache file
                yyyymmdd = s.strftime("%Y%m%d")
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_fs_details_{yyyymmdd}.csv.gz"
                if (cache_dir != "") and os.path.isfile(
                    f"{cache_dir}/{yyyy}/{cache_file}"
                ):
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}", dtype=str)
                    df["DisclosedDate"] = pd.to_datetime(
                        df["DisclosedDate"], format="%Y-%m-%d"
                    )
                    buff.append(df)
                else:
                    future = executor.submit(
                        self.get_fins_fs_details, date_yyyymmdd=yyyymmdd
                    )
                    futures[future] = yyyymmdd
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
                yyyymmdd = futures[future]
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_fs_details_{yyyymmdd}.csv.gz"
                if cache_dir != "":
                    # create year directory
                    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
                    # write cache file
                    df.to_csv(f"{cache_dir}/{yyyy}/{cache_file}", index=False)

        return pd.concat(buff).sort_values(
            ["DisclosedDate", "DisclosedTime", "LocalCode"]
        )

    def get_fins_dividend(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        get information on dividends (determined and forecast) per share of listed companies etc.. API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: information on dividends data (Sorted by "Code")
        """
        return self._fins_dividend_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_dividend_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        配当金データを日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 配当金データ(Code, AnnouncementDate, AnnouncementTime列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_fins_dividend, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(
            ["AnnouncementDate", "AnnouncementTime", "Code"]
        )

    def get_fins_announcement(self) -> pd.DataFrame:
        """
        get fin announcement

        Args:
            N/A

        Returns:
            pd.DataFrame: Schedule of financial announcement
        """
        return self._fins_announcement_api.execute(self)

    def get_option_index_option(
        self,
        date_yyyymmdd,
    ) -> pd.DataFrame:
        """
        get information on the OHLC etc. of Nikkei 225 API returns

        Args:
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame:
                Nikkei 225 Options' OHLC etc. (Sorted by "Code")
        """
        return self._option_index_option_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_index_option_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        指数オプション（Nikkei225）に関するOHLC等の情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 指数オプション（Nikkei225）に関するOHLC等 (Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_option_index_option, date_yyyymmdd=s.strftime("%Y-%m-%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_markets_trading_calendar(
        self,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        取引カレンダーを取得

        Args:
            holiday_division: 休日区分
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日

        Returns:
            pd.DataFrame: 取り引きカレンダー (Date列でソートされています)
        """
        return self._markets_trading_calendar_api.execute(
            self,
            holiday_division=holiday_division,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )

    def get_derivatives_futures(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        get information on the OHLC etc. of Futures API returns

        Args:
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame:
                Futures' OHLC etc. (Sorted by "Code")
        """
        return self._derivatives_futures_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
            category=category,
            contract_flag=contract_flag,
        )

    def get_derivatives_futures_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
        category: str = "",
        contract_flag: str = "",
    ) -> pd.DataFrame:
        """
        先物に関するOHLC等の情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 先物に関するOHLC等 (Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_derivatives_futures,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_derivatives_options(
        self,
        date_yyyymmdd: str,
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        get information on the OHLC etc. of Option API returns

        Args:
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame:
                Futures' OHLC etc. (Sorted by "Code")
        """
        return self._derivatives_options_api.execute(
            self,
            date_yyyymmdd=date_yyyymmdd,
            category=category,
            contract_flag=contract_flag,
            code=code,
        )

    def get_derivatives_options_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
        category: str = "",
        contract_flag: str = "",
        code: str = "",
    ) -> pd.DataFrame:
        """
        オプションに関するOHLC等の情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: オプションに関するOHLC等 (Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            options = [
                executor.submit(
                    self.get_derivatives_options,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                    category=category,
                    contract_flag=contract_flag,
                    code=code,
                )
                for s in dates
            ]
            for option in as_completed(options):
                df = option.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_markets_short_selling_positions(
        self,
        code: str = "",
        disclosed_date: str = "",
        disclosed_date_from: str = "",
        disclosed_date_to: str = "",
        calculated_date: str = "",
    ) -> pd.DataFrame:
        """
        get short selling positions API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock
                will be obtained for the issue on which both common and preferred stocks
                are listed.
            disclosed_date: disclosed date (e.g. 20240301 or 2024-03-01)
            disclosed_date_from: disclosed date from (e.g. 20240301 or 2024-03-01)
            disclosed_date_to: disclosed date to (e.g. 20240301 or 2024-03-01)
            calculated_date: calculated date (e.g. 20240301 or 2024-03-01)
        Returns:
            pd.DataFrame: short selling positions (Sorted by "DisclosedDate",
            "CalculatedDate", and "Code" columns)
        """
        return self._markets_short_selling_positions_api.execute(
            self,
            code=code,
            disclosed_date=disclosed_date,
            disclosed_date_from=disclosed_date_from,
            disclosed_date_to=disclosed_date_to,
            calculated_date=calculated_date,
        )

    def get_markets_short_selling_positions_range(
        self,
        start_dt: DatetimeLike = "20131107",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        空売り残高報告データを日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 空売り残高報告データ (DisclosedDate, CalculatedDate,
            Code列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_markets_short_selling_positions,
                    disclosed_date=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["DisclosedDate", "CalculatedDate", "Code"])

    def get_markets_daily_margin_interest(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        get daily margin interest API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-character issue code is specified, only the data of common stock
                will be obtained for the issue on which both common and preferred stocks
                are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: daily margin interest (Sorted by "Code" and "PublishedDate" columns)
        """
        return self._markets_daily_margin_interest_api.execute(
            self,
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

    def get_daily_margin_interest_range(
        self,
        start_dt: DatetimeLike = "20080508",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全銘柄の日々公表信用取引残高を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 日々公表信用取引残高 (Code, PublishedDate列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_markets_daily_margin_interest,
                    date_yyyymmdd=s.strftime("%Y-%m-%d"),
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "PublishedDate"])
