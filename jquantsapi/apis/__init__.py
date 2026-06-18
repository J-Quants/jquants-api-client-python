"""
API ごとの実装クラス群をまとめるパッケージ。

- 共通の抽象クラスは `BaseApi`
- v2 向けの実装は `jquantsapi.apis.v2`
"""

from .base import BaseApi  # noqa: F401
