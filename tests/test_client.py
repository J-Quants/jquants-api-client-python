from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from dateutil import tz

import jquantsapi


@pytest.mark.parametrize(
    "mail_address, password, refresh_token,"
    "env, isfile, load,"
    "exp_mail_address, exp_password, exp_refresh_token, exp_refresh_token_expire",
    (
        (
            None,
            None,
            None,
            {},
            [False, False, False, False],
            [],
            "",
            "",
            "",
            0,
        ),
        (
            None,
            None,
            None,
            {},
            [True, False, True, False],
            [{"dummy": {"mail_address": "mail_address"}}, {}],
            "",
            "",
            "",
            0,
        ),
        (
            None,
            None,
            None,
            {"JQUANTS_API_CLIENT_CONFIG_FILE": ""},
            [True, True, True, True],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address",
                        "password": "password",
                    }
                },
                {},
                {},
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address_overwrite",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_overwrite",
            "password",
            "refresh_token",
            6,
        ),
        (
            None,
            None,
            None,
            {
                "JQUANTS_API_MAIL_ADDRESS": "mail_address_env",
                "JQUANTS_API_PASSWORD": "password_env",
                "JQUANTS_API_REFRESH_TOKEN": "refresh_token_env",
            },
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_env",
            "password_env",
            "refresh_token_env",
            6,
        ),
        (
            "mail",
            "password",
            None,
            {},
            [False, False, False, False],
            [],
            "mail",
            "password",
            "",
            0,
        ),
        (
            "mail_address_param",
            "password_param",
            "refresh_token_param",
            {},
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_param",
            "password_param",
            "refresh_token_param",
            6,
        ),
    ),
)
def test_client(
    refresh_token,
    mail_address,
    password,
    env,
    isfile,
    load,
    exp_mail_address,
    exp_password,
    exp_refresh_token,
    exp_refresh_token_expire,
):
    utcnow = pd.Timestamp("2022-09-08T22:00:00Z")
    with (
        patch.object(jquantsapi.Client, "_is_colab", return_value=True),
        patch.object(jquantsapi.client.os.path, "isfile", side_effect=isfile),
        patch("builtins.open"),
        patch.dict(jquantsapi.client.os.environ, env, clear=True),
        patch.object(jquantsapi.client.tomllib, "load", side_effect=load),
        patch.object(jquantsapi.client.pd.Timestamp, "utcnow", return_value=utcnow),
    ):
        cli = jquantsapi.Client(
            refresh_token=refresh_token, mail_address=mail_address, password=password
        )
        assert cli._mail_address == exp_mail_address
        assert cli._password == exp_password
        assert cli._refresh_token == exp_refresh_token
        assert cli._refresh_token_expire == utcnow + pd.Timedelta(
            exp_refresh_token_expire, unit="D"
        )


def test_get_price_range():
    """
    get_price_range()を呼ぶ際、引数に様々な型が入っていても問題なく
    get_prices_daily_quotes()に単一の年月日が渡される事を確認する。
    """
    # テストではHTTP通信を行わず、
    # get_id_token()およびget_prices_daily_quotes()をモックすることで、
    # 実際に通信が発生しないようにする。
    mock = MagicMock(return_value=pd.DataFrame(columns=["Code", "Date"]))
    cli = jquantsapi.Client(refresh_token="dummy")
    cli.get_id_token = MagicMock()
    cli.get_prices_daily_quotes = mock  # get_prices_daily_quotes() をモックに置き換える
    formats = {
        # テストする期間はいつでも良いので、何かが起こりやすい閏日をテスト対象に含む
        "str_8digits": ("20200227", "20200302"),
        "str_split_by_hyphen": ("2020-02-27", "2020-03-02"),
        "datetime_without_tz": (datetime(2020, 2, 27), datetime(2020, 3, 2)),
        "datetime_with_tz": (
            datetime(2020, 2, 27, tzinfo=tz.gettz("Asia/Tokyo")),
            datetime(2020, 3, 2, tzinfo=tz.gettz("Asia/Tokyo")),
        ),
        "pd.Timestamp": (pd.Timestamp("2020-02-27"), pd.Timestamp("2020-03-02")),
    }

    for _fmt, (start, end) in formats.items():
        cli.get_price_range(start, end)

        # 呼び出しの履歴と、get_prices_daily_quotes()が呼ばれた際の年月日8桁の引数を比較
        assert mock.mock_calls == [
            call.get_prices_daily_quotes(date_yyyymmdd="20200227"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200228"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200229"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200301"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200302"),
        ]
        mock.reset_mock()
