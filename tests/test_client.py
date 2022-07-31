from src.client import JQuantsAPIClient


def test_jquants_api_client():
    cli = JQuantsAPIClient(refresh_token="hello")
    print("Hello world")
    assert True
