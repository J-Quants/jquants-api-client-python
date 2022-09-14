import os
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
    ref. https://jpx.gitbook.io/j-quants-api/
    """

    JQUANTS_API_BASE = "https://api.jpx-jquants.com/v1"
    MAX_WORKERS = 5

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
            refresh_token_expiredat: refresh token expired_at
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
            raise ValueError("mail_address must contain '@' charactor.")

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
        headers = {"Authorization": f"Bearer {id_token}"}
        return headers

    def _request_session(
        self,
        status_forcelist: Optional[List[int]] = None,
        method_whitelist: Optional[List[str]] = None,
    ) -> requests.Session:
        """
        requests の session 取得

        リトライを設定

        Args:
            status_forcelist: リトライ対象のステータスコード
            method_whitelist: リトライ対象のメソッド
        Returns:
            requests.session
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
        if method_whitelist is None:
            method_whitelist = ["HEAD", "GET", "OPTIONS", "POST"]

        if self._session is None:
            retry_strategy = Retry(
                total=3,
                status_forcelist=status_forcelist,
                method_whitelist=method_whitelist,
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

        ヘッダーにアクセストークンを設定
        タイムアウトを設定

        Args:
            url: アクセスするURL
            payload: 送信するデータ
            headers: HTTPヘッダ

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        ret = s.post(url, data=data, json=json, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

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
            raise ValueError("mail_address must contain '@' charactor.")

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
        Retruns:
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
            # - error is 400 bad request
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

    def get_listed_info(self, code: str = "") -> pd.DataFrame:
        """
        銘柄一覧を取得

        Args:
            code: 銘柄コード (Optional)

        Returns:
            pd.DataFrame: 銘柄一覧
        """
        url = f"{self.JQUANTS_API_BASE}/listed/info"
        params = {"code": code}
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["info"])
        cols = [
            "Code",
            "CompanyName",
            "CompanyNameEnglish",
            "CompanyNameFull",
            "SectorCode",
            "UpdateDate",
            "MarketCode",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)

        df.loc[:, "UpdateDate"] = pd.to_datetime(df["UpdateDate"], format="%Y%m%d")
        df.sort_values("Code", inplace=True)

        return df[cols]

    def get_listed_sections(self) -> pd.DataFrame:
        """
        セクター一覧を取得

        Args:
            N/A

        Returns:
            pd.DataFrame: セクター一覧
        """
        url = f"{self.JQUANTS_API_BASE}/listed/sections"
        params: dict = {}
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["sections"])
        cols = ["SectorCode", "SectorName"]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.sort_values("SectorCode", inplace=True)
        return df[cols]

    @staticmethod
    def get_market_segments() -> pd.DataFrame:
        """
        市場区分一覧を取得

        Args:
            N/A

        Returns:
            pd.DataFrame: 市場区分一覧
        """

        df = pd.DataFrame(
            [
                {"MarketCode": "", "MarketName": "東証非上場"},
                {"MarketCode": "1", "MarketName": "市場一部"},
                {"MarketCode": "2", "MarketName": "市場二部"},
                {"MarketCode": "3", "MarketName": "マザーズ"},
                {"MarketCode": "5", "MarketName": "その他"},
                {"MarketCode": "6", "MarketName": "JASDAQ スタンダード"},
                {"MarketCode": "7", "MarketName": "JASDAQ グロース"},
                {"MarketCode": "8", "MarketName": "TOKYO PRO Market"},
                {"MarketCode": "9", "MarketName": "上場廃止"},
                {"MarketCode": "A", "MarketName": "プライム"},
                {"MarketCode": "B", "MarketName": "スタンダード"},
                {"MarketCode": "C", "MarketName": "グロース"},
            ]
        )
        cols = ["MarketCode", "MarketName"]
        df.sort_values("MarketCode", inplace=True)
        return df[cols]

    def get_list(self) -> pd.DataFrame:
        """
        銘柄一覧を取得 (市場区分およびセクター結合済み)

        Args:
            N/A

        Returns:
            pd.DataFrame: 銘柄一覧
        """
        df_list = self.get_listed_info()
        df_sectors = self.get_listed_sections()
        df_segments = self.get_market_segments()

        df_list = pd.merge(df_list, df_sectors, how="left", on=["SectorCode"])
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
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["daily_quotes"])
        cols = [
            "Code",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "TurnoverValue",
            "AdjustmentFactor",
            "AdjustmentOpen",
            "AdjustmentHigh",
            "AdjustmentLow",
            "AdjustmentClose",
            "AdjustmentVolume",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
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
                    self.get_prices_daily_quotes, date_yyyymmdd=s.strftime("%Y%m%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)

        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_fins_statements(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務情報取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務情報 (DisclosedUnixTime列、DisclosureNumber列でソートされています)
        """
        url = f"{self.JQUANTS_API_BASE}/fins/statements"
        params = {
            "code": code,
            "date": date_yyyymmdd,
        }
        ret = self._get(url, params)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["statements"])
        cols = [
            "DisclosureNumber",
            "DisclosedDate",
            "ApplyingOfSpecificAccountingOfTheQuarterlyFinancialStatements",
            "AverageNumberOfShares",
            "BookValuePerShare",
            "ChangesBasedOnRevisionsOfAccountingStandard",
            "ChangesInAccountingEstimates",
            "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
            "CurrentFiscalYearEndDate",
            "CurrentFiscalYearStartDate",
            "CurrentPeriodEndDate",
            "DisclosedTime",
            "DisclosedUnixTime",
            "EarningsPerShare",
            "Equity",
            "EquityToAssetRatio",
            "ForecastDividendPerShare1stQuarter",
            "ForecastDividendPerShare2ndQuarter",
            "ForecastDividendPerShare3rdQuarter",
            "ForecastDividendPerShareAnnual",
            "ForecastDividendPerShareFiscalYearEnd",
            "ForecastEarningsPerShare",
            "ForecastNetSales",
            "ForecastOperatingProfit",
            "ForecastOrdinaryProfit",
            "ForecastProfit",
            "LocalCode",
            "MaterialChangesInSubsidiaries",
            "NetSales",
            "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
            "NumberOfTreasuryStockAtTheEndOfFiscalYear",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
            "ResultDividendPerShare1stQuarter",
            "ResultDividendPerShare2ndQuarter",
            "ResultDividendPerShare3rdQuarter",
            "ResultDividendPerShareAnnual",
            "ResultDividendPerShareFiscalYearEnd",
            "RetrospectiveRestatement",
            "TotalAssets",
            "TypeOfCurrentPeriod",
            "TypeOfDocument",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "DisclosedDate"] = pd.to_datetime(
            df["DisclosedDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentPeriodEndDate"] = pd.to_datetime(
            df["CurrentPeriodEndDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentFiscalYearStartDate"] = pd.to_datetime(
            df["CurrentFiscalYearStartDate"], format="%Y-%m-%d"
        )
        df.loc[:, "CurrentFiscalYearEndDate"] = pd.to_datetime(
            df["CurrentFiscalYearEndDate"], format="%Y-%m-%d"
        )
        df.sort_values(["DisclosedUnixTime", "DisclosureNumber"], inplace=True)
        return df[cols]

    def get_fins_announcement(self) -> pd.DataFrame:
        """
        翌日の決算発表情報の取得

        Args:
            N/A

        Returns:
            pd.DataFrame: 翌日決算発表情報
        """
        url = f"{self.JQUANTS_API_BASE}/fins/announcement"
        ret = self._get(url)
        d = ret.json()
        df = pd.DataFrame.from_dict(d["announcement"])
        cols = [
            "Code",
            "Date",
            "CompanyName",
            "FiscalYear",
            "SectorName",
            "FiscalQuarter",
            "Section",
        ]
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.loc[:, "Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]

    def get_statements_range(
        self,
        start_dt: DatetimeLike = "20170101",
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
            pd.DataFrame: 財務情報 (DisclosedUnixTime列、DisclosureNumber列でソートされています)
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
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}")
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

        return pd.concat(buff).sort_values(["DisclosedUnixTime", "DisclosureNumber"])
