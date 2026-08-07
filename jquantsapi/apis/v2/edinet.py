from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore

from jquantsapi.apis.base import BaseApi, SupportsRequest


def _fetch_edinet(
    client: SupportsRequest,
    path: str,
    *,
    edinet_code: str = "",
    code: str = "",
    date_yyyymmdd: str = "",
) -> pd.DataFrame:
    """
    EDINET 系 3 エンドポイント共通の取得処理。

    クエリ仕様は 3 エンドポイントで共通:
    - edinet_code / code / date は任意指定（すべて省略時は API 実行日提出分）
    - edinet_code と code の同時指定は不可（API 仕様では 400 エラー）
    - ネスト項目（Hldrs / Report 等）は dict / list のまま object 列として保持する
    """
    if edinet_code and code:
        raise ValueError("edinet_code と code は同時に指定できません。")

    params: dict[str, Any] = {}
    if edinet_code:
        params["edinet_code"] = edinet_code
    if code:
        params["code"] = code
    if date_yyyymmdd:
        params["date"] = date_yyyymmdd

    all_data = client._get_paginated(  # type: ignore[attr-defined]
        path,
        params=params,
    )

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_data)
    for col in ("SubDate", "PerSt", "PerEn"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    sort_cols = [c for c in ["SubDate", "SubTime", "Code"] if c in df.columns]
    if sort_cols:
        df.sort_values(sort_cols, inplace=True)
    return df.reset_index(drop=True)


class EdinetMajorShareholdersApiV2(BaseApi):
    """
    v2 の大株主状況 API (`/edinet/major-shareholders`) のラッパークラス。
    """

    name = "edinet_major_shareholders"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/edinet/major-shareholders` を実行し、大株主状況を DataFrame で返す。

        大株主レコードは `Hldrs` 列に list のまま保持されます。
        """
        return _fetch_edinet(
            client,
            "/edinet/major-shareholders",
            edinet_code=edinet_code,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )


class EdinetCrossShareholdingsApiV2(BaseApi):
    """
    v2 の政策保有株式 API (`/edinet/cross-shareholdings`) のラッパークラス。
    """

    name = "edinet_cross_shareholdings"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/edinet/cross-shareholdings` を実行し、政策保有株式を DataFrame で返す。

        保有主体ブロックは `Report` / `Largest` / `SecondLargest` 列に
        dict のまま保持されます（内部に Spec[] / Deem[] の銘柄明細を含む）。
        """
        return _fetch_edinet(
            client,
            "/edinet/cross-shareholdings",
            edinet_code=edinet_code,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )


class EdinetLargeVolumeShareholdersApiV2(BaseApi):
    """
    v2 の大量保有報告書 API (`/edinet/large-volume-shareholders`) のラッパークラス。
    """

    name = "edinet_large_volume_shareholders"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        edinet_code: str = "",
        code: str = "",
        date_yyyymmdd: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        `/edinet/large-volume-shareholders` を実行し、大量保有報告書データを
        DataFrame で返す。

        提出者及び共同保有者のレコードは `Hldrs` 列に list のまま保持されます。
        """
        return _fetch_edinet(
            client,
            "/edinet/large-volume-shareholders",
            edinet_code=edinet_code,
            code=code,
            date_yyyymmdd=date_yyyymmdd,
        )
