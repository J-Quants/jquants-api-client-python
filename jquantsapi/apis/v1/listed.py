from __future__ import annotations

import json
from typing import Any

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class ListedInfoApiV1(BaseApi):
    """
    v1 の上場銘柄一覧 API (`/listed/info`) のラッパークラス。
    """

    name = "listed_info"
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
        v1 `/listed/info` を実行し、上場銘柄情報を DataFrame で返す。

        Args:
            client: v1 `Client` インスタンスを想定
            code: 銘柄コード (任意)
            date_yyyymmdd: 基準日 (YYYYMMDD or YYYY-MM-DD, 任意)
        """
        url = f"{client.JQUANTS_API_BASE}/listed/info"

        # ページングしながら全件取得
        all_info: list[dict[str, Any]] = []
        base_params: dict[str, Any] = {}
        if code != "":
            base_params["code"] = code
        if date_yyyymmdd != "":
            base_params["date"] = date_yyyymmdd

        pagination_key = ""
        while True:
            params = dict(base_params)
            if pagination_key != "":
                params["pagination_key"] = pagination_key

            resp = client._get(url, params)  # type: ignore[arg-type]
            resp.encoding = client.RAW_ENCODING
            payload = json.loads(resp.text)

            data = payload.get("info", [])
            if isinstance(data, list):
                all_info.extend(data)

            pagination_key = payload.get("pagination_key", "")
            if not pagination_key:
                break

        df = pd.DataFrame.from_dict(all_info)

        standard_premium_flag = "MarginCode" in df.columns
        if standard_premium_flag:
            cols = constants.LISTED_INFO_STANDARD_PREMIUM_COLUMNS
        else:
            cols = constants.LISTED_INFO_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values("Code", inplace=True)
        return df[cols].reset_index(drop=True)
