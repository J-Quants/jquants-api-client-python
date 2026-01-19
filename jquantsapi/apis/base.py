from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol

import pandas as pd  # type: ignore


class SupportsRequest(Protocol):
    """
    Api クラスが利用するクライアント側の最小インタフェース。

    - v1 の `Client`
    - v2 の `ClientV2`
    などがこの Protocol を満たす想定。
    """

    JQUANTS_API_BASE: str
    RAW_ENCODING: str

    def _get(
        self, url: str, params: dict[str, Any] | None = None
    ):  # pragma: no cover - Protocol 定義のみ
        ...


class BaseApi(ABC):
    """
    各エンドポイント単位の API 実装のための抽象クラス。

    v1 / v2 の実装で共通の execute インタフェースを提供する。
    """

    #: 論理名 (例: "listed_info")
    name: str
    #: バージョン識別子 (例: "v1", "v2")
    version: str

    @abstractmethod
    def execute(self, client: SupportsRequest, **params: Any) -> pd.DataFrame:
        """
        実際に API を実行し、結果を DataFrame で返す。

        Args:
            client: HTTP リクエストなどを実行するクライアント
            **params: API ごとのパラメータ
        """
