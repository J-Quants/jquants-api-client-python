from __future__ import annotations

from typing import Any, Optional

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class FinSummaryApiV2(BaseApi):
    """
    v2 の財務情報サマリ API (`/fins/summary`) のラッパークラス。
    """

    name = "fin_summary"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        cursor: str = "",
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, Optional[str]]:
        """
        v2 `/fins/summary` を実行し、財務情報サマリと cursor を返す。

        cursor は API 仕様上、最終ページのレスポンスにのみ含まれます。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/summary"  # type: ignore[attr-defined]

        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        if cursor:
            params["cursor"] = cursor

        all_data: list[dict[str, Any]] = []
        returned_cursor: Optional[str] = None
        query = dict(params)

        while True:
            resp = client._get(url, query)  # type: ignore[attr-defined]
            payload = resp.json()
            all_data.extend(payload.get("data", []))
            returned_cursor = payload.get("cursor")

            pagination_key = payload.get("pagination_key")
            if not pagination_key:
                break
            query["pagination_key"] = pagination_key

        cols = constants.FIN_SUMMARY_COLUMNS_V2
        if not all_data:
            return pd.DataFrame(columns=cols), returned_cursor

        df = pd.DataFrame.from_records(all_data)
        for col in (
            "DiscDate",
            "CurPerSt",
            "CurPerEn",
            "CurFYSt",
            "CurFYEn",
            "NxtFYSt",
            "NxtFYEn",
        ):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)

        return df[cols].reset_index(drop=True), returned_cursor


class FinDetailsApiV2(BaseApi):
    """
    v2 の財務諸表詳細 API (`/fins/details`) のラッパークラス。
    """

    name = "fin_details"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        code: str = "",
        date_yyyymmdd: str = "",
        cursor: str = "",
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, Optional[str]]:
        """
        v2 `/fins/details` を実行し、財務諸表詳細と cursor を返す。

        cursor は API 仕様上、最終ページのレスポンスにのみ含まれます。
        """
        url = f"{client.JQUANTS_API_BASE}/fins/details"  # type: ignore[attr-defined]

        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_yyyymmdd:
            params["date"] = date_yyyymmdd
        if cursor:
            params["cursor"] = cursor

        all_data: list[dict[str, Any]] = []
        returned_cursor: Optional[str] = None
        query = dict(params)

        while True:
            resp = client._get(url, query)  # type: ignore[attr-defined]
            payload = resp.json()
            all_data.extend(payload.get("data", []))
            returned_cursor = payload.get("cursor")

            pagination_key = payload.get("pagination_key")
            if not pagination_key:
                break
            query["pagination_key"] = pagination_key

        if not all_data:
            return pd.DataFrame(), returned_cursor

        df = pd.DataFrame.from_records(all_data)
        if "DiscDate" in df.columns:
            df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")
        sort_cols = [c for c in ["DiscDate", "DiscTime", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True), returned_cursor


class FinDividendApiV2(BaseApi):
    """
    v2 の配当金情報 API (`/fins/dividend`) のラッパークラス。
    """

    name = "fin_dividend"
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
        `/fins/dividend` を実行し、配当金情報を DataFrame で返す。
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
            "/fins/dividend",
            params=params,
        )

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(all_data)
        if "PubDate" in df.columns:
            df["PubDate"] = pd.to_datetime(df["PubDate"], errors="coerce")
        sort_cols = [c for c in ["PubDate", "Code"] if c in df.columns]
        if sort_cols:
            df.sort_values(sort_cols, inplace=True)
        return df.reset_index(drop=True)
