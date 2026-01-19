from __future__ import annotations

import json
from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class FinsStatementsApiV1(BaseApi):
    """
    v1 の財務情報 API (`/fins/statements`) のラッパークラス。
    """

    name = "fins_statements"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/fins/statements` を実行し、財務情報を DataFrame で返す。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/statements"  # type: ignore[attr-defined]
        params: dict[str, Any] = {"code": code, "date": date_yyyymmdd}

        data: list[dict[str, Any]] = []
        pagination_key: str = ""
        while True:
            req_params = dict(params)
            if pagination_key != "":
                req_params["pagination_key"] = pagination_key

            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d: dict[str, Any] = json.loads(resp.text)
            page = d.get("statements", [])
            if isinstance(page, list):
                data.extend(page)

            pagination_key = d.get("pagination_key", "")
            if not pagination_key:
                break
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


class FinsFsDetailsApiV1(BaseApi):
    """
    v1 の財務諸表(BS/PL) API (`/fins/fs_details`) のラッパークラス。
    """

    name = "fins_fs_details"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/fins/fs_details` を実行し、財務諸表(BS/PL)を DataFrame で返す。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/fs_details"  # type: ignore[attr-defined]
        params: dict[str, Any] = {"code": code, "date": date_yyyymmdd}

        data: list[dict[str, Any]] = []
        pagination_key: str = ""
        while True:
            req_params = dict(params)
            if pagination_key != "":
                req_params["pagination_key"] = pagination_key

            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d: dict[str, Any] = json.loads(resp.text)
            page = d.get("fs_details", [])
            if isinstance(page, list):
                data.extend(page)

            pagination_key = d.get("pagination_key", "")
            if not pagination_key:
                break
        df = pd.json_normalize(data=data)
        cols = constants.FINS_FS_DETAILS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"], format="%Y-%m-%d")
        df.sort_values(["DisclosedDate", "DisclosedTime", "LocalCode"], inplace=True)
        return df


class FinsDividendApiV1(BaseApi):
    """
    v1 の配当金情報 API (`/fins/dividend`) のラッパークラス。
    """

    name = "fins_dividend"
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
        `/fins/dividend` を実行し、配当金情報を DataFrame で返す。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/dividend"  # type: ignore[attr-defined]
        params: dict[str, Any] = {"code": code}
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd

        data: list[dict[str, Any]] = []
        pagination_key: str = ""
        while True:
            req_params = dict(params)
            if pagination_key != "":
                req_params["pagination_key"] = pagination_key

            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d: dict[str, Any] = json.loads(resp.text)
            page = d.get("dividend", [])
            if isinstance(page, list):
                data.extend(page)

            pagination_key = d.get("pagination_key", "")
            if not pagination_key:
                break
        df = pd.DataFrame.from_dict(data)
        cols = constants.FINS_DIVIDEND_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["AnnouncementDate"] = pd.to_datetime(
            df["AnnouncementDate"], format="%Y-%m-%d"
        )
        df.sort_values(["Code"], inplace=True)
        return df[cols]


class FinsAnnouncementApiV1(BaseApi):
    """
    v1 の決算発表予定 API (`/fins/announcement`) のラッパークラス。
    """

    name = "fins_announcement"
    version = "v1"

    def execute(
        self,
        client: SupportsRequest,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/fins/announcement` を実行し、決算発表予定データを DataFrame で返す。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/announcement"  # type: ignore[attr-defined]
        params: dict[str, Any] = {}

        data: list[dict[str, Any]] = []
        pagination_key: str = ""
        while True:
            req_params = dict(params)
            if pagination_key != "":
                req_params["pagination_key"] = pagination_key

            resp = client._get(url, req_params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING  # type: ignore[attr-defined]
            d: dict[str, Any] = json.loads(resp.text)
            page = d.get("announcement", [])
            if isinstance(page, list):
                data.extend(page)

            pagination_key = d.get("pagination_key", "")
            if not pagination_key:
                break
        df = pd.DataFrame.from_dict(data)
        cols = constants.FINS_ANNOUNCEMENT_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]
