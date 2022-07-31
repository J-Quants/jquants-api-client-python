import jquantsapi


def test_jquants_api_client():
    cli = jquantsapi.Client(refresh_token="hello")
    print("Hello world")
    print(cli)
    assert True
