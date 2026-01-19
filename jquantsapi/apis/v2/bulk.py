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
    """

    name = "bulk_list"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        endpoint: Union[str, BulkEndpoint] = "",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        v2 `/bulk/list` を実行し、取得可能なデータ一覧を DataFrame で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            endpoint: 取得したいデータのエンドポイント (例: "/equities/master")
        """
        url = f"{client.JQUANTS_API_BASE}/bulk/list"

        # BulkEndpointの場合はvalue(str)を取得
        endpoint_str = (
            endpoint.value if isinstance(endpoint, BulkEndpoint) else endpoint
        )

        params: dict[str, Any] = {"endpoint": endpoint_str}

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
        **kwargs: Any,
    ) -> str:
        """
        v2 `/bulk/get` を実行し、ダウンロードURLを取得する。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            key: BulkListで取得したKey

        Returns:
            str: ダウンロードURL
        """
        url = f"{client.JQUANTS_API_BASE}/bulk/get"

        params: dict[str, Any] = {"key": key}

        resp = client._get(url, params)  # type: ignore[arg-type]
        payload = resp.json()

        return payload.get("url", "")
