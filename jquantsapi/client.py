import json
import os
import platform
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, List, Mapping, Optional, Union

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

from jquantsapi import constants, enums

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


DatetimeLike = Union[datetime, pd.Timestamp, str]
_Data = Union[str, Mapping[str, Any]]


class TokenAuthRefreshBadRequestException(Exception):
    pass


class Client:
    """
    J-Quants API からデータを取得する
    ref. https://jpx.gitbook.io/j-quants-ja/
    """

    JQUANTS_API_BASE = "https://api.jquants.com/v1"
    MAX_WORKERS = 5
    USER_AGENT = "jqapi-python"
    USER_AGENT_VERSION = "0.0.0"
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
        status_forcelist: Optional[List[int]] = None,
        allowed_methods: Optional[List[str]] = None,
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
        requests の get 用ラッパー

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

    # /listed
    def _get_listed_info_raw(
        self, code: str = "", date_yyyymmdd: str = "", pagination_key: str = ""
    ) -> str:
        """
        Get listed companies raw API returns

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)
            pagination_key: ページングキー

        Returns:
            str: listed companies raw json string
        """
        url = f"{self.JQUANTS_API_BASE}/listed/info"
        params = {}
        if code != "":
            params["code"] = code
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_listed_info(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        Get listed companies

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            pd.DataFrame: listed companies (sorted by Code)
        """
        j = self._get_listed_info_raw(code=code, date_yyyymmdd=date_yyyymmdd)
        d = json.loads(j)
        data = d["info"]
        while "pagination_key" in d:
            j = self._get_listed_info_raw(
                code=code,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["info"]
        df = pd.DataFrame.from_dict(data)

        standard_premium_flag = "MarginCode" in df.columns
        if standard_premium_flag:
            cols = constants.LISTED_INFO_STANDARD_PREMIUM_COLUMNS
        else:
            cols = constants.LISTED_INFO_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values("Code", inplace=True)
        return df[cols]

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

    # /prices
    def _get_prices_daily_quotes_raw(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get daily quotes raw API returns

        Args:
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日
            pagination_key: ページングキー

        Returns:
            str: daily quotes
        """
        url = f"{self.JQUANTS_API_BASE}/prices/daily_quotes"
        params = {
            "code": code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_prices_daily_quotes_raw(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        data = d["daily_quotes"]
        while "pagination_key" in d:
            j = self._get_prices_daily_quotes_raw(
                code=code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["daily_quotes"]
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

    def _get_prices_prices_am_raw(
        self,
        code: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get the morning session's high, low, opening, and closing prices for individual stocks raw API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            pagination_key: ページングキー

        Returns:
            str: the morning session's OHLC data
        """
        url = f"{self.JQUANTS_API_BASE}/prices/prices_am"
        params = {
            "code": code,
        }
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_prices_prices_am(
        self,
        code: str = "",
    ) -> pd.DataFrame:
        """
        get the morning session's high, low, opening, and closing prices for individual stocks API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
        Returns: pd.DataFrame: the morning session's OHLC data
        """
        j = self._get_prices_prices_am_raw(
            code=code,
        )
        d = json.loads(j)
        if d.get("message"):
            return d["message"]
        data = d["prices_am"]
        while "pagination_key" in d:
            j = self._get_prices_prices_am_raw(
                code=code,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["prices_am"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.PRICES_PRICES_AM_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]

    # /markets
    def _get_markets_trades_spec_raw(
        self,
        section: Union[str, enums.MARKET_API_SECTIONS] = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        Weekly Trading by Type of Investors raw API returns

        Args:
            section: section name (e.g. "TSEPrime" or MARKET_API_SECTIONS.TSEPrime)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー

        Returns:
            str: Weekly Trading by Type of Investors
        """
        url = f"{self.JQUANTS_API_BASE}/markets/trades_spec"
        params = {}
        if section != "":
            params["section"] = section
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_markets_trades_spec_raw(
            section=section, from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        d = json.loads(j)
        data = d["trades_spec"]
        while "pagination_key" in d:
            j = self._get_markets_trades_spec_raw(
                section=section,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
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

    def _get_markets_weekly_margin_interest_raw(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get weekly margin interest raw API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: weekly margin interest
        """
        url = f"{self.JQUANTS_API_BASE}/markets/weekly_margin_interest"
        params = {
            "code": code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: weekly margin interest (Sorted by "Date" and "Code" columns)
        """
        j = self._get_markets_weekly_margin_interest_raw(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        data = d["weekly_margin_interest"]
        while "pagination_key" in d:
            j = self._get_markets_weekly_margin_interest_raw(
                code=code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["weekly_margin_interest"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKETS_WEEKLY_MARGIN_INTEREST
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]

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

    def _get_markets_short_selling_raw(
        self,
        sector_33_code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get daily short sale ratios and trading value by industry (sector) raw API returns

        Args:
            sector_33_code: 33-sector code (e.g. 0050 or 8050)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: daily short sale ratios and trading value by industry
        """
        url = f"{self.JQUANTS_API_BASE}/markets/short_selling"
        params = {
            "sector33code": sector_33_code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = date_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_markets_short_selling_raw(
            sector_33_code=sector_33_code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )

        d = json.loads(j)
        data = d["short_selling"]
        while "pagination_key" in d:
            j = self._get_markets_short_selling_raw(
                sector_33_code=sector_33_code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["short_selling"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKET_SHORT_SELLING_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Sector33Code"], inplace=True)
        return df[cols]

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

    def _get_markets_breakdown_raw(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get detail breakdown trading data raw API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: detail breakdown trading data
        """
        url = f"{self.JQUANTS_API_BASE}/markets/breakdown"
        params = {
            "code": code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: detail breakdown trading data (Sorted by "Code")
        """
        j = self._get_markets_breakdown_raw(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        data = d["breakdown"]
        while "pagination_key" in d:
            j = self._get_markets_breakdown_raw(
                code=code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["breakdown"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.MARKETS_BREAKDOWN_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Code"], inplace=True)
        return df[cols]

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
    def _get_indices_topix_raw(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        TOPIX Daily OHLC raw API returns

        Args:
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: TOPIX Daily OHLC
        """
        url = f"{self.JQUANTS_API_BASE}/indices/topix"
        params = {}
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_indices_topix_raw(
            from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        d = json.loads(j)
        data = d["topix"]
        while "pagination_key" in d:
            j = self._get_indices_topix_raw(
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["topix"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.INDICES_TOPIX_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]

    # /fins
    def _get_fins_statements_raw(
        self, code: str = "", date_yyyymmdd: str = "", pagination_key: str = ""
    ) -> str:
        """
        get fins statements raw API return

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)
            pagination_key: ページングキー

        Returns:
            str: fins statements
        """
        url = f"{self.JQUANTS_API_BASE}/fins/statements"
        params = {
            "code": code,
            "date": date_yyyymmdd,
        }
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING

        return ret.text

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
        j = self._get_fins_statements_raw(code=code, date_yyyymmdd=date_yyyymmdd)
        d = json.loads(j)
        data = d["statements"]
        while "pagination_key" in d:
            j = self._get_fins_statements_raw(
                code=code,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["statements"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.FINS_STATEMENTS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"], format="%Y-%m-%d")
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
        df.sort_values(["DisclosedDate", "DisclosedTime", "LocalCode"], inplace=True)
        return df[cols]

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

    def _get_fins_fs_details_raw(
        self, code: str = "", date_yyyymmdd: str = "", pagination_key: str = ""
    ) -> str:
        """
        get fins fs_details raw API return

        Args:
            code: 銘柄コード
            date_yyyymmdd: 開示日(YYYYMMDD or YYYY-MM-DD)
            pagination_key: ページングキー

        Returns:
            str: fins fs_details
        """
        url = f"{self.JQUANTS_API_BASE}/fins/fs_details"
        params = {
            "code": code,
            "date": date_yyyymmdd,
        }
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING

        return ret.text

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
        j = self._get_fins_fs_details_raw(code=code, date_yyyymmdd=date_yyyymmdd)
        d = json.loads(j)
        data = d["fs_details"]
        while "pagination_key" in d:
            j = self._get_fins_fs_details_raw(
                code=code,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["fs_details"]
        df = pd.json_normalize(data=data)
        cols = constants.FINS_FS_DETAILS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"], format="%Y-%m-%d")
        df.sort_values(["DisclosedDate", "DisclosedTime", "LocalCode"], inplace=True)
        return df

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

    def _get_fins_dividend_raw(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
        pagination_key: str = "",
    ) -> str:
        """
        get  information on dividends (determined and forecast) per share of listed companies etc.. raw API returns

        Args:
            code: issue code (e.g. 27800 or 2780)
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: information on dividends data
        """
        url = f"{self.JQUANTS_API_BASE}/fins/dividend"
        params = {
            "code": code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
                If a 4-digit issue code is specified, only the data of common stock will be obtained
                for the issue on which both common and preferred stocks are listed.
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: information on dividends data (Sorted by "Code")
        """
        j = self._get_fins_dividend_raw(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        data = d["dividend"]
        while "pagination_key" in d:
            j = self._get_fins_dividend_raw(
                code=code,
                from_yyyymmdd=from_yyyymmdd,
                to_yyyymmdd=to_yyyymmdd,
                date_yyyymmdd=date_yyyymmdd,
                pagination_key=d["pagination_key"],
            )
            d = json.loads(j)
            data += d["dividend"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.FINS_DIVIDEND_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["AnnouncementDate"] = pd.to_datetime(
            df["AnnouncementDate"], format="%Y-%m-%d"
        )
        df.sort_values(["Code"], inplace=True)
        return df[cols]

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

    def _get_fins_announcement_raw(
        self,
        pagination_key: str = "",
    ) -> str:
        """
        get fin announcement raw API returns

        Args:
            pagination_key: ページングキー

        Returns:
            str: Schedule of financial announcement
        """
        url = f"{self.JQUANTS_API_BASE}/fins/announcement"
        params = {}
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_fins_announcement(self) -> pd.DataFrame:
        """
        get fin announcement

        Args:
            N/A

        Returns:
            pd.DataFrame: Schedule of financial announcement
        """
        j = self._get_fins_announcement_raw()
        d = json.loads(j)
        data = d["announcement"]
        while "pagination_key" in d:
            j = self._get_fins_announcement_raw(pagination_key=d["pagination_key"])
            d = json.loads(j)
            data += d["announcement"]
        df = pd.DataFrame.from_dict(data)
        cols = constants.FINS_ANNOUNCEMENT_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]

    # /option
    def _get_option_index_option_raw(
        self,
        date_yyyymmdd,
        pagination_key: str = "",
    ) -> str:
        """
        get information on the OHLC etc. of Nikkei 225 raw API returns

        Args:
            date_yyyymmdd: date of data (e.g. 20210907 or 2021-09-07)
            pagination_key: ページングキー
        Returns:
            str: Nikkei 225 Options' OHLC etc.
        """
        url = f"{self.JQUANTS_API_BASE}/option/index_option"
        params = {
            "date": date_yyyymmdd,
        }
        if pagination_key != "":
            params["pagination_key"] = pagination_key
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_option_index_option_raw(
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        data = d["index_option"]
        while "pagination_key" in d:
            j = self._get_option_index_option_raw(
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

    # /trading_calendar
    def _get_markets_trading_calendar_raw(
        self,
        holiday_division: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> str:
        """
        get trading calendar raw API returns

        Args:
            holiday_division: 休日区分
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日

        Returns:
            str: trading calendar
        """
        url = f"{self.JQUANTS_API_BASE}/markets/trading_calendar"
        params = {}
        if holiday_division != "":
            params["holidaydivision"] = holiday_division
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

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
        j = self._get_markets_trading_calendar_raw(
            holiday_division=holiday_division,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
        )
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["trading_calendar"])
        cols = constants.MARKETS_TRADING_CALENDAR
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]
