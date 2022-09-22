from contextlib import nullcontext as does_not_raise
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from dateutil import tz

import jquantsapi


@pytest.mark.parametrize(
    "mail_address, password, refresh_token,"
    "env, isfile, load,"
    "exp_mail_address, exp_password, exp_refresh_token, exp_refresh_token_expire,"
    "exp_raise",
    (
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
            pytest.raises(ValueError),
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
                        "mail_address": "mail_address@",
                        "password": "password",
                    }
                },
                {},
                {},
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address_overwrite@",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_overwrite@",
            "password",
            "refresh_token",
            6,
            does_not_raise(),
        ),
        (
            None,
            None,
            None,
            {
                "JQUANTS_API_MAIL_ADDRESS": "mail_address_env@",
                "JQUANTS_API_PASSWORD": "password_env",
                "JQUANTS_API_REFRESH_TOKEN": "refresh_token_env",
            },
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address@",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_env@",
            "password_env",
            "refresh_token_env",
            6,
            does_not_raise(),
        ),
        (
            "mail@",
            "password",
            None,
            {},
            [False, False, False, False],
            [],
            "mail@",
            "password",
            "",
            0,
            does_not_raise(),
        ),
        (
            "mail@",
            None,
            None,
            {},
            [False, False, False, False],
            [],
            "mail@",
            "",
            "",
            0,
            pytest.raises(ValueError),
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
            pytest.raises(ValueError),
        ),
        (
            None,
            None,
            "token",
            {},
            [False, False, False, False],
            [],
            "",
            "",
            "token",
            6,
            does_not_raise(),
        ),
        (
            "mail_address_param@",
            "password_param",
            "refresh_token_param",
            {},
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address@",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_param@",
            "password_param",
            "refresh_token_param",
            6,
            does_not_raise(),
        ),
    ),
)
def test_client(
    mail_address,
    password,
    refresh_token,
    env,
    isfile,
    load,
    exp_mail_address,
    exp_password,
    exp_refresh_token,
    exp_refresh_token_expire,
    exp_raise,
):
    utcnow = pd.Timestamp("2022-09-08T22:00:00Z")
    with exp_raise, patch.object(
        jquantsapi.Client, "_is_colab", return_value=True
    ), patch.object(jquantsapi.client.os.path, "isfile", side_effect=isfile), patch(
        "builtins.open"
    ), patch.dict(
        jquantsapi.client.os.environ, env, clear=True
    ), patch.object(
        jquantsapi.client.tomllib, "load", side_effect=load
    ), patch.object(
        jquantsapi.client.pd.Timestamp, "utcnow", return_value=utcnow
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


@pytest.mark.parametrize(
    "init_mail_address, init_password, param_mail_address, param_password, exp_raise",
    (
        # use private variable via constructor
        ("m@", "p", None, None, does_not_raise()),
        ("", "", None, None, pytest.raises(ValueError)),
        ("m", "p", None, None, pytest.raises(ValueError)),
        # use parameter
        (None, None, "m@", "p", does_not_raise()),
        (None, None, "", "", pytest.raises(ValueError)),
        (None, None, "m", "p", pytest.raises(ValueError)),
        # overwrite
        ("m@", "p", "", "", pytest.raises(ValueError)),
        ("m@", "p", "m", "p", pytest.raises(ValueError)),
    ),
)
def test_get_refresh_token(
    init_mail_address, init_password, param_mail_address, param_password, exp_raise
):
    config = {
        "mail_address": "",
        "password": "",
        "refresh_token": "dummy_token",
    }

    with exp_raise, patch.object(
        jquantsapi.Client, "_load_config", return_value=config
    ), patch.object(jquantsapi.Client, "_post") as mock_post:
        mock_post.return_value.json.return_value = {"refreshToken": "ret_token"}

        cli = jquantsapi.Client(
            refresh_token="dummy_token",
            mail_address=init_mail_address,
            password=init_password,
        )
        # overwrite expire time
        cli._refresh_token_expire = pd.Timestamp.utcnow()
        ret = cli.get_refresh_token(param_mail_address, param_password)
        assert ret == "ret_token"


@pytest.mark.parametrize(
    "section, from_yyyymmdd, to_yyyymmdd, exp_params",
    (
        ("", "", "", {}),
        ("TSEPrime", "", "", {"section": "TSEPrime"}),
        (
            jquantsapi.MARKET_API_SECTIONS.TSE1st,
            "",
            "",
            {"section": "TSE1st"},
        ),
        ("", "20220101", "", {"from": "20220101"}),
        ("", "", "20220101", {"to": "20220101"}),
        (
            "TSEPrime",
            "20220101",
            "20220102",
            {"section": "TSEPrime", "from": "20220101", "to": "20220102"},
        ),
    ),
)
def test_get_markets_trades_spec(section, from_yyyymmdd, to_yyyymmdd, exp_params):
    config = {
        "mail_address": "",
        "password": "",
        "refresh_token": "dummy_token",
    }

    ret_value = {"trades_spec": []}
    exp_ret_len = 0
    exp_raise = does_not_raise()

    with exp_raise, patch.object(
        jquantsapi.Client, "_load_config", return_value=config
    ), patch.object(jquantsapi.Client, "_get") as mock_get:
        mock_get.return_value.json.return_value = ret_value

        cli = jquantsapi.Client()
        ret = cli.get_markets_trades_spec(
            section=section, from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        args, _ = mock_get.call_args
        assert args[1] == exp_params
        assert len(ret) == exp_ret_len


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
