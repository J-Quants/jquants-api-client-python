# jquants-api-client

[![PyPI version](https://badge.fury.io/py/jquants-api-client.svg)](https://badge.fury.io/py/jquants-api-client)

個人投資家向けデータAPI配信サービス「 [J-Quants API](https://jpx-jquants.com/#jquants-api) 」のPythonクライアントライブラリです。
J-QuantsやAPI仕様についての詳細を知りたい方は [公式ウェブサイト](https://jpx-jquants.com/) をご参照ください。
現在、J-Quants APIはベータ版サービスとして提供されています。

## 使用方法

pip経由でインストールします。

```shell
pip install jquants-api-client
```

### J-Quants API の利用

To use J-Quants API, you need to "Applications for J-Quants API" from [J-Quants API Web site](https://jpx-jquants.com/?lang=en#jquants-api).

J-Quants APIを利用するためには[J-Quants API の Web サイト](https://jpx-jquants.com/#jquants-api) から「J-Quants API申し込み」が必要になります。

jquants-api-client-python を使用するためには「J-Quants API ログインページで使用するメールアドレスおよびパスワード」または「J-Quants API メニューページから取得したリフレッシュトークン」が必要になります。必要に応じて下記のWebサイトより取得してください。

[J-Quants API ログインページ](https://application.jpx-jquants.com/)

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

APIレスポンスがDataframeの形式で取得できます。

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

## 対応API

### ラッパー群　 

J-Quants API の各APIエンドポイントに対応しています。

  - get_refresh_token
  - get_id_token
  - get_listed_info
  - get_listed_sections
  - get_prices_daily_quotes
  - get_indices_topix
  - get_markets_trades_spec
  - get_fins_statements
  - get_fins_announcement

### ユーティリティ群

業種や市場区分一覧などを返します。
  - get_market_segments
  - get_17_sectors
  - get_33_sectors

日付範囲を指定して一括でデータ取得して、取得したデータを結合して返すようなユーティリティが用意されています。

  - get_list
  - get_price_range
  - get_statements_range

## 設定

認証用のメールアドレス/パスワードおよびリフレッシュトークンは設定ファイルおよび環境変数を使用して指定することも可能です。
設定は下記の順に読み込まれ、設定項目が重複している場合は後に読み込まれた値で上書きされます。

1. `/content/drive/MyDrive/drive_ws/secret/jquants-api.toml` (Google Colabのみ)
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

Google Colab および Python 3.10 で動作確認を行っています。
J-Quants APIは現在β版のため、本ライブラリも今後仕様が変更となる可能性があります。

## 開発

J-Quants API Clientの開発に是非ご協力ください。
Github上でIssueやPull Requestをお待ちしております。
