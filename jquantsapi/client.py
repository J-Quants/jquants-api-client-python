import os
from datetime import datetime
from typing import Dict

import pandas as pd  # type: ignore
import requests
from dateutil import tz
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class Client:
    """
    J-Quants API からデータを取得する
    ref. https://jpx.gitbook.io/j-quants-api/
    """

    JQUANTS_API_BASE = "https://api.jpx-jquants.com/v1"

    def __init__(self, refresh_token: str) -> None:
        """
        Args:
            refresh_token: J-Quants API リフレッシュトークン
        """
        self.refresh_token = refresh_token
        self._id_token = ""
        self._id_token_expire = pd.Timestamp.utcnow()

    def _base_headers(self) -> dict:
        """
        J-Quants API にアクセスする際にヘッダーにIDトークンを設定
        """
        headers = {"Authorization": f"Bearer {self.get_id_token()}"}
        return headers

    @staticmethod
    def _request_session(
        status_forcelist=None,
        method_whitelist=None,
    ):
        """
        requests の session 取得

        リトライを設定

        Args:
            N/A
        Returns:
            requests.session
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
        if method_whitelist is None:
            method_whitelist = ["HEAD", "GET", "OPTIONS"]

        retry_strategy = Retry(
            total=3,
            status_forcelist=status_forcelist,
            method_whitelist=method_whitelist,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        s = requests.Session()
        s.mount("https://", adapter)
        # s.mount("http://", adapter)
        return s

    def _get(self, url: str, params: dict = None) -> requests.Response:
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
        self, url: str, payload: dict = None, headers: dict = None
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
        s = self._request_session(method_whitelist=["POST"])

        ret = s.post(url, data=payload, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

    def get_id_token(self) -> str:
        if self._id_token_expire > pd.Timestamp.utcnow():
            return self._id_token

        url = f"{self.JQUANTS_API_BASE}/token/auth_refresh?refreshtoken={self.refresh_token}"
        ret = self._post(url)
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
        params: Dict = {}
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
            pd.DataFrame: 株価情報
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
        start_dt: datetime = datetime(2017, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
    ) -> pd.DataFrame:
        """
        全銘柄の株価情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 株価情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            df = self.get_prices_daily_quotes(date_yyyymmdd=s.strftime("%Y%m%d"))
            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        return pd.concat(buff)

    def get_fins_statements(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務情報取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務情報
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
        df.sort_values("DisclosedUnixTime", inplace=True)
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
        start_dt: datetime = datetime(2017, 1, 1, tzinfo=tz.gettz("Asia/Tokyo")),
        end_dt: datetime = datetime.now(tz.gettz("Asia/Tokyo")),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ

        Returns:
            pd.DataFrame: 財務情報
        """
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        counter = 1
        for s in dates:
            # fetch data via API or cache file
            cache_file = f"fins_statements_{s.strftime('%Y%m%d')}.csv.gz"
            if (cache_dir != "") and os.path.isfile(
                f"{cache_dir}/{s.strftime('%Y')}/{cache_file}"
            ):
                df = pd.read_csv(f"{cache_dir}/{s.strftime('%Y')}/{cache_file}")
            else:
                df = self.get_fins_statements(date_yyyymmdd=s.strftime("%Y%m%d"))
                if cache_dir != "":
                    # create year directory
                    os.makedirs(f"{cache_dir}/{s.strftime('%Y')}", exist_ok=True)
                    # write cache file
                    df.to_csv(
                        f"{cache_dir}/{s.strftime('%Y')}/{cache_file}", index=False
                    )

            buff.append(df)
            # progress log
            if (counter % 100) == 0:
                print(f"{counter} / {len(dates)}")
            counter += 1
        return pd.concat(buff)
