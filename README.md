# jquants-api-client

[![PyPI version](https://badge.fury.io/py/jquants-api-client.svg)](https://badge.fury.io/py/jquants-api-client)

個人投資家向けデータ API 配信サービス「 [J-Quants API](https://jpx-jquants.com/#jquants-api) 」の Python クライアントライブラリです。
J-Quants や API 仕様についての詳細を知りたい方は [公式ウェブサイト](https://jpx-jquants.com/) をご参照ください。
現在、J-Quants API は有償版サービスとして提供されています。

## 使用方法

pip 経由でインストールします。

```shell
pip install jquants-api-client
```

### J-Quants API の利用

To use J-Quants API, you need to "Applications for J-Quants API" from [J-Quants API Web site](https://jpx-jquants.com/?lang=en) and to select a plan.

J-Quants API を利用するためには[J-Quants API の Web サイト](https://jpx-jquants.com/) から「J-Quants API 申し込み」及び利用プランの選択が必要になります。

jquants-api-client-python を使用するためには「J-Quants API ログインページで使用するメールアドレスおよびパスワード」または「J-Quants API メニューページから取得したリフレッシュトークン」が必要になります。必要に応じて下記の Web サイトより取得してください。

[J-Quants API ログインページ](https://jpx-jquants.com/auth/signin/)

### サンプルコード

```python
from datetime import datetime
from dateutil import tz
import jquantsapi

my_mail_address:str = "*****"
my_password: str = "*****"
cli = jquantsapi.Client(mail_address=my_mail_address, password=my_password)
df = cli.get_price_range(
    start_dt=datetime(2022, 7, 25, tzinfo=tz.gettz("Asia/Tokyo")),
    end_dt=datetime(2022, 7, 26, tzinfo=tz.gettz("Asia/Tokyo")),
)
print(df)
```

API レスポンスが Dataframe の形式で取得できます。

```shell
       Code       Date  ...  AdjustmentClose  AdjustmentVolume
0     13010 2022-07-25  ...           3630.0            8100.0
1     13050 2022-07-25  ...           2023.0           54410.0
2     13060 2022-07-25  ...           2001.0          943830.0
3     13080 2022-07-25  ...           1977.5          121300.0
4     13090 2022-07-25  ...          43300.0             391.0
...     ...        ...  ...              ...               ...
4189  99930 2022-07-26  ...           1426.0            5600.0
4190  99940 2022-07-26  ...           2605.0            7300.0
4191  99950 2022-07-26  ...            404.0           13000.0
4192  99960 2022-07-26  ...           1255.0            4000.0
4193  99970 2022-07-26  ...            825.0          133600.0

[8388 rows x 14 columns]
```

より具体的な使用例は [サンプルノートブック(/examples)](examples) をご参照ください。

## 対応 API

### ラッパー群　

J-Quants API の各 API エンドポイントに対応しています。

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

------------------ Premium plan or higher is required ------------------

- get_markets_breakdown
- get_prices_prices_am
- get_fins_dividend

### ユーティリティ群

業種や市場区分一覧などを返します。

- get_market_segments
- get_17_sectors
- get_33_sectors

日付範囲を指定して一括でデータ取得して、取得したデータを結合して返すようなユーティリティが用意されています。

------------------ Free plan or higher is required ------------------

- get_list
- get_price_range
- get_statements_range

------------------ Standard plan or higher is required ------------------

- get_weekly_margin_range
- get_short_selling_range
- get_index_option_range

------------------ Premium plan or higher is required ------------------

- get_breakdown_range
- get_dividend_range

## 設定

認証用のメールアドレス/パスワードおよびリフレッシュトークンは設定ファイルおよび環境変数を使用して指定することも可能です。
設定は下記の順に読み込まれ、設定項目が重複している場合は後に読み込まれた値で上書きされます。

1. `/content/drive/MyDrive/drive_ws/secret/jquants-api.toml` (Google Colab のみ)
2. `${HOME}/.jquants-api/jquants-api.toml`
3. `jquants-api.toml`
4. `os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]`
5. `${JQUANTS_API_MAIL_ADDRESS}`, `${JQUANTS_API_PASSWORD}`, `${JQUANTS_API_REFRESH_TOKEN}`

### 設定ファイル例

`jquants-api.toml` は下記のように設定します。

```toml
[jquants-api-client]
mail_address = "*****"
password = "*****"
refresh_token = "*****"
```

## 動作確認

Google Colab および Python 3.11 で動作確認を行っています。
J-Quants API は有償版で継続開発されているため、本ライブラリも今後仕様が変更となる可能性があります。
Python 3.7 サポートは廃止予定です。将来のバージョンではサポート対象外となります。
Please note Python 3.7 support is deprecated.

## 開発

J-Quants API Client の開発に是非ご協力ください。
Github 上で Issue や Pull Request をお待ちしております。
