from __future__ import annotations

from typing import Any, Optional

import pandas as pd  # type: ignore

from jquantsapi import constants
from jquantsapi.apis.base import BaseApi, SupportsRequest


class TdListApiV2(BaseApi):
    """
    v2 の TDnet/適時開示インデックス一覧 API (`/td/list`) のラッパークラス。

    `date` または `code` のどちらかは必須です。
    """

    name = "td_list"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        date: str = "",
        code: str = "",
        from_date: str = "",
        to_date: str = "",
        disc_items: str = "",
        cursor: str = "",
        **kwargs: Any,
    ) -> tuple[pd.DataFrame, Optional[str]]:
        """
        v2 `/td/list` を実行し、(適時開示インデックス一覧, cursor) を返す。

        pagination_key が返された場合は自動的に全件取得します。
        cursor はレスポンスの最終ページに含まれる場合に返却されます（含まれない場合は None）。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            date: 開示日 (YYYYMMDD or YYYY-MM-DD)。`code` と排他的に使用します。
            code: 銘柄コード。`date` と排他的に使用します。
            from_date: 取得開始日。`code` と組み合わせて使用します。
            to_date: 取得終了日。`code` と組み合わせて使用します。
            disc_items: 公開項目コード（カンマ区切りで複数指定可能）。
            cursor: 前回レスポンスで返却された cursor。差分取得に使用します。
                    `pagination_key` と同時指定不可。
        Returns:
            tuple[pd.DataFrame, Optional[str]]:
                - DataFrame: 取得した開示一覧
                - cursor: レスポンスに含まれる cursor（含まれない場合は None）
        """
        url = f"{client.JQUANTS_API_BASE}/td/list"

        params: dict[str, Any] = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if disc_items:
            params["discItems"] = disc_items
        if cursor:
            params["cursor"] = cursor

        all_data: list[dict[str, Any]] = []
        returned_cursor: Optional[str] = None
        query = dict(params)

        while True:
            resp = client._get(url, query)  # type: ignore[arg-type]
            payload = resp.json()
            all_data.extend(payload.get("data", []))
            returned_cursor = payload.get("cursor")

            pagination_key = payload.get("pagination_key")
            if not pagination_key:
                break
            query["pagination_key"] = pagination_key

        cols = constants.TD_LIST_COLUMNS_V2
        if not all_data:
            return pd.DataFrame(columns=cols), returned_cursor

        df = pd.DataFrame.from_records(all_data)
        if "DiscDate" in df.columns:
            df["DiscDate"] = pd.to_datetime(df["DiscDate"], errors="coerce")

        return df[cols].reset_index(drop=True), returned_cursor


class TdFilesApiV2(BaseApi):
    """
    v2 の TDnet/適時開示ファイル取得 API (`/td/files`) のラッパークラス。

    開示番号に対応するファイルのダウンロードURLを取得します。
    """

    name = "td_files"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        *,
        disc_no: str,
        docs: str = "",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        v2 `/td/files` を実行し、ダウンロードURL情報を dict で返す。

        Args:
            client: v2 `ClientV2` インスタンスを想定
            disc_no: 開示番号（14桁）
            docs: 取得するファイル種別（カンマ区切り: g/s/x）。省略時は全種別。

        Returns:
            dict: ``discNo`` と ``files`` (pdf/summaryPdf/xbrl の URL) を含む辞書
        """
        url = f"{client.JQUANTS_API_BASE}/td/files"

        params: dict[str, Any] = {"discNo": disc_no}
        if docs:
            params["docs"] = docs

        resp = client._get(url, params)  # type: ignore[arg-type]
        return resp.json()


class TdBulkApiV2(BaseApi):
    """
    v2 の TDnet/適時開示インデックス一括ダウンロード API (`/td/bulk`) のラッパークラス。

    過去5年分の適時開示インデックスを収録した CSV (gzip) のダウンロードURLと
    最終更新日時を取得します。
    """

    name = "td_bulk"
    version = "v2"

    def execute(
        self,
        client: SupportsRequest,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        v2 `/td/bulk` を実行し、一括ダウンロード情報を dict で返す。

        Returns:
            dict: ``lastUpdated`` と ``url`` を含む辞書
        """
        url = f"{client.JQUANTS_API_BASE}/td/bulk"
        resp = client._get(url, {})  # type: ignore[arg-type]
        return resp.json()


