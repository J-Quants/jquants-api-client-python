# jquants-api-client

[![PyPI version](https://badge.fury.io/py/jquants-api-client.svg)](https://badge.fury.io/py/jquants-api-client)

個人投資家向けデータ API 配信サービス「 [J-Quants API](https://jpx-jquants.com/) 」の Python クライアントライブラリです。
J-Quants や API 仕様についての詳細を知りたい方は [公式ウェブサイト](https://jpx-jquants.com/) をご参照ください。
現在、J-Quants API は有償版サービスとして提供されています。

## 使用方法

pip 経由でインストールします。

```shell
pip install jquants-api-client
```

### J-Quants API の利用

To use J-Quants API, you need to "Applications for J-Quants API" from [J-Quants API Web site](https://jpx-jquants.com/) and to select a plan.

J-Quants API を利用するためには[J-Quants API の Web サイト](https://jpx-jquants.com/) から「J-Quants API 申し込み」及び利用プランの選択が必要になります。

jquants-api-client-python を使用するためには「J-Quants API ログインページで使用するメールアドレスおよびパスワード」または「J-Quants API メニューページから取得したリフレッシュトークン」が必要になります。必要に応じて下記の Web サイトより取得してください。

[J-Quants API ログインページ](https://jpx-jquants.com/login)

### サンプルコード (V2)

V2 API では API キーによる認証を使用します。API キーは [J-Quants API ダッシュボード](https://jpx-jquants.com/dashboard/api-keys) から取得できます。

```python
from datetime import datetime
from dateutil import tz
import jquantsapi

my_api_key: str = "*****"
cli = jquantsapi.ClientV2(api_key=my_api_key)
df = cli.get_eq_bars_daily_range(
    start_dt=datetime(2022, 7, 25, tzinfo=tz.gettz("Asia/Tokyo")),
    end_dt=datetime(2022, 7, 26, tzinfo=tz.gettz("Asia/Tokyo")),
)
print(df)
```

環境変数 `JQUANTS_API_KEY` を設定している場合は、引数を省略できます。

```python
import jquantsapi

cli = jquantsapi.ClientV2()  # 環境変数 JQUANTS_API_KEY を使用
```

API レスポンスが Dataframe の形式で取得できます。

```shell
      Code       Date  ...     AdjC       AdjVo
0    13010 2022-07-25  ...   3630.0      8100.0
1    13050 2022-07-25  ...   2023.0     54410.0
2    13060 2022-07-25  ...   2001.0    943830.0
3    13080 2022-07-25  ...   1977.5    121300.0
4    13090 2022-07-25  ...  43300.0       391.0
...    ...        ...  ...      ...         ...
4189 99930 2022-07-26  ...   1426.0      5600.0
4190 99940 2022-07-26  ...   2605.0      7300.0
4191 99950 2022-07-26  ...    404.0     13000.0
4192 99960 2022-07-26  ...   1255.0      4000.0
4193 99970 2022-07-26  ...    825.0    133600.0

[8388 rows x 14 columns]
```

より具体的な使用例は [サンプルノートブック(/examples)](examples) をご参照ください。

## 対応 API (V2)

`ClientV2` クラスで利用可能な V2 API エンドポイントです。

### ラッパー群

------------------ Free plan or higher is required ------------------

- get_eq_master - 上場銘柄一覧
- get_eq_bars_daily - 株価日足
- get_fin_summary - 決算サマリー
- get_eq_earnings_cal - 決算発表日

------------------ Light plan or higher is required ------------------

- get_idx_bars_daily - 指数日足
- get_idx_bars_daily_topix - TOPIX日足
- get_mkt_calendar - 営業日カレンダー
- get_bulk_list - バルクデータ一覧
- get_bulk - バルクデータ取得

------------------ Standard plan or higher is required ------------------

- get_mkt_short_ratio - 空売り比率
- get_mkt_short_sale_report - 空売り報告
- get_mkt_margin_interest - 週次信用取引残高
- get_mkt_margin_alert - 信用規制情報
- get_drv_bars_daily_fut - 先物日足
- get_drv_bars_daily_opt - オプション日足
- get_drv_bars_daily_opt_225 - 日経225オプション日足

------------------ Premium plan or higher is required ------------------

- get_mkt_breakdown - 売買内訳
- get_eq_bars_daily_am - 株価午前終値
- get_eq_investor_types - 投資部門別売買状況
- get_fin_details - 財務詳細
- get_fin_dividend - 配当情報

------------------ Minute Bar Addon is required ------------------

- get_eq_bars_minute - 分足
- get_eq_bars_5minute - 5分足（分足から算出）
- get_eq_bars_15minute - 15分足（分足から算出）

### ユーティリティ群

業種や市場区分一覧などを返します。

- get_market_segments - 市場区分一覧
- get_17_sectors - 17業種一覧
- get_33_sectors - 33業種一覧

日付範囲を指定して一括でデータ取得して、取得したデータを結合して返すユーティリティです。

------------------ Free plan or higher is required ------------------

- get_list - 銘柄一覧（セクター情報付き）
- get_eq_bars_daily_range - 株価日足（範囲指定）
- get_fin_summary_range - 決算サマリー（範囲指定）

------------------ Standard plan or higher is required ------------------

- get_mkt_short_ratio_range - 空売り比率（範囲指定）
- get_mkt_short_sale_report_range - 空売り報告（範囲指定）
- get_mkt_margin_interest_range - 信用取引残高（範囲指定）
- get_mkt_margin_alert_range - 信用規制情報（範囲指定）
- get_drv_bars_daily_fut_range - 先物日足（範囲指定）
- get_drv_bars_daily_opt_range - オプション日足（範囲指定）
- get_drv_bars_daily_opt_225_range - 日経225オプション日足（範囲指定）

------------------ Premium plan or higher is required ------------------

- get_mkt_breakdown_range - 売買内訳（範囲指定）
- get_fin_details_range - 財務詳細（範囲指定）

## 対応 API (V1) - Deprecated

> **⚠️ 非推奨**: `Client` クラス (V1) は非推奨となりました。今後は `ClientV2` をご利用ください。
> V1 API は将来のバージョンで削除される予定です。

<details>
<summary>V1 API 一覧（クリックで展開）</summary>

### ラッパー群

------------------ Free plan or higher is required ------------------

- get_refresh_token
- get_id_token
- get_listed_info
- get_prices_daily_quotes
- get_fins_statements
- get_fins_announcement

------------------ Light plan or higher is required ------------------

- get_markets_trades_spec
- get_indices_topix

------------------ Standard plan or higher is required ------------------

- get_option_index_option
- get_markets_weekly_margin_interest
- get_markets_short_selling
- get_indices
- get_markets_short_selling_positions
- get_markets_daily_margin_interest

------------------ Premium plan or higher is required ------------------

- get_markets_breakdown
- get_prices_prices_am
- get_fins_dividend
- get_fins_fs_details
- get_derivatives_futures
- get_derivatives_options

### ユーティリティ群

- get_market_segments
- get_17_sectors
- get_33_sectors

------------------ Free plan or higher is required ------------------

- get_list
- get_price_range
- get_statements_range

------------------ Standard plan or higher is required ------------------

- get_weekly_margin_range
- get_short_selling_range
- get_index_option_range
- get_markets_short_selling_positions_range
- get_daily_margin_interest_range

------------------ Premium plan or higher is required ------------------

- get_breakdown_range
- get_dividend_range
- get_fins_fs_details_range
- get_derivatives_futures_range
- get_derivatives_options_range

</details>

## レートリミット

J-Quants API には、サービスの安定稼働を目的としてレートリミット（利用頻度の制限）が設けられています。
プランごとのレートリミットの詳細については、[公式ドキュメント](https://jpx-jquants.com/spec/rate-limits) をご参照ください。

### 注意事項

サフィックスが `_range` で終わるメソッド（例: `get_eq_bars_daily_range`、`get_fin_summary_range` など）は、指定された日付範囲に対して並列処理で繰り返し API リクエストを行います。
そのため、広い日付範囲を指定した場合や、短時間に複数回実行した場合、レートリミットに達する可能性があります。

レートリミットを超過すると、API は HTTP ステータスコード `429 Too Many Requests` を返します。
エラーが発生した場合は、一定時間待機してから再試行するか、より狭い日付範囲で分割してリクエストすることをご検討ください。

## 設定

### V2 (ClientV2)

API キーは設定ファイルおよび環境変数を使用して指定することも可能です。
設定は下記の順に読み込まれ、設定項目が重複している場合は後に読み込まれた値で上書きされます。

1. `/content/drive/MyDrive/drive_ws/secret/jquants-api.toml` (Google Colab のみ)
2. `${HOME}/.jquants-api/jquants-api.toml`
3. `jquants-api.toml`
4. `os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]`
5. `${JQUANTS_API_KEY}`

#### 設定ファイル例

`jquants-api.toml` は下記のように設定します。

```toml
[jquants-api-client]
api_key = "*****"
```

### V1 (Client) - Deprecated

<details>
<summary>V1 設定方法（クリックで展開）</summary>

認証用のメールアドレス/パスワードおよびリフレッシュトークンは設定ファイルおよび環境変数を使用して指定することも可能です。
設定は下記の順に読み込まれ、設定項目が重複している場合は後に読み込まれた値で上書きされます。

1. `/content/drive/MyDrive/drive_ws/secret/jquants-api.toml` (Google Colab のみ)
2. `${HOME}/.jquants-api/jquants-api.toml`
3. `jquants-api.toml`
4. `os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]`
5. `${JQUANTS_API_MAIL_ADDRESS}`, `${JQUANTS_API_PASSWORD}`, `${JQUANTS_API_REFRESH_TOKEN}`

#### 設定ファイル例

`jquants-api.toml` は下記のように設定します。

```toml
[jquants-api-client]
mail_address = "*****"
password = "*****"
refresh_token = "*****"
```

</details>

## 動作確認

Google Colab および Python 3.13 で動作確認を行っています。
J-Quants API は有償版で継続開発されているため、本ライブラリも今後仕様が変更となる可能性があります。
Python の EOL を迎えたバージョンはサポート対象外となります。
Please note we only support Python supported versions. Unsupported versions (after EOL) are not supported.
ref. https://devguide.python.org/versions/#supported-versions

## 開発

J-Quants API Client の開発に是非ご協力ください。
Github 上で Issue や Pull Request をお待ちしております。
