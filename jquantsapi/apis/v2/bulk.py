from __future__ import annotations

from typing import Any, Union

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest
from jquantsapi.enums import BulkEndpoint


class BulkListApiV2(BaseApi):
    """
    v2 の Bulk List API (`/bulk/list`) のラッパークラス。

    指定したエンドポイントで取得可能なデータ一覧を取得します。
    `endpoint` または `date` のどちらかは必須です。
    """

    name = "bulk_list"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        endpoint: Union[str, BulkEndpoint] = "",
        date: str = "",
        from_date: str = "",
        to_date: str = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        v2 `/bulk/list` を実行し、取得可能なデータ一覧を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            endpoint: 取得したいデータのエンドポイント (例: "/equities/master")。
                      `date` と排他的に使用します。
            date: 対象日付 (YYYY-MM, YYYYMM, YYYY-MM-DD, YYYYMMDD)。
                  `endpoint` と排他的に使用します。
            from_date: 取得期間の開始日。`endpoint` 指定時のみ使用可能。
            to_date: 取得期間の終了日。`endpoint` 指定時のみ使用可能。
        """
        url = f"{client.JQUANTS_API_BASE}/bulk/list"

        # BulkEndpointの場合はvalue(str)を取得
        endpoint_str = (
            endpoint.value if isinstance(endpoint, BulkEndpoint) else endpoint
        )

        params: dict[str, Any] = {}
        if endpoint_str:
            params["endpoint"] = endpoint_str
        if date:
            params["date"] = date
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        resp = client._get(url, params)  # type: ignore[arg-type]
        payload = resp.json()

        data = payload.get("data", [])
        cols = constants.BULK_LIST_COLUMNS_V2

        if not data:
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame.from_records(data)
        if "LastModified" in df.columns:
            df["LastModified"] = pd.to_datetime(df["LastModified"], errors="coerce")

        return df[cols].reset_index(drop=True)


class BulkGetApiV2(BaseApi):
    """
    v2 の Bulk Get API (`/bulk/get`) のラッパークラス。

    指定したキーのデータをダウンロードするためのURLを取得します。
    """

    name = "bulk_get"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        key: str = "",
        endpoint: Union[str, BulkEndpoint] = "",
        date: str = "",
        **kwargs: Any,
    ) -> str:
        """
        v2 `/bulk/get` を実行し、ダウンロードURLを取得する。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            key: BulkListで取得したKey。`endpoint` + `date` と排他的に使用します。
            endpoint: 取得するデータのエンドポイント名。`date` と組み合わせて使用します。
            date: 対象日付 (YYYY-MM, YYYYMM, YYYY-MM-DD, YYYYMMDD)。`endpoint` と組み合わせて使用します。

        Returns:
            str: ダウンロードURL
        """
        url = f"{client.JQUANTS_API_BASE}/bulk/get"

        # BulkEndpointの場合はvalue(str)を取得
        endpoint_str = (
            endpoint.value if isinstance(endpoint, BulkEndpoint) else endpoint
        )

        params: dict[str, Any] = {}
        if key:
            params["key"] = key
        if endpoint_str:
            params["endpoint"] = endpoint_str
        if date:
            params["date"] = date

        resp = client._get(url, params)  # type: ignore[arg-type]
        payload = resp.json()

        return payload.get("url", "")
