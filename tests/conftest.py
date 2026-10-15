import os

import pytest


@pytest.fixture
def sample_payment():
    return {
        "amount": "120.50", "currency": "RUB", "description": "Order 42",
        "metadata": {"order_id": 42}, "webhook_url": "https://example.org/hook",
    }


@pytest.fixture
def integration_urls():
    url = os.getenv("TEST_DATABASE_URL")
    rabbit = os.getenv("TEST_RABBITMQ_URL")
    if not url or not rabbit:
        pytest.skip("Set TEST_DATABASE_URL and TEST_RABBITMQ_URL for integration tests")
    return url, rabbit


@pytest.fixture(autouse=True)
def test_secrets(monkeypatch):
    from app.config import get_settings
    monkeypatch.setenv("API_KEY", "unit-test-api-key")
    monkeypatch.setenv("WEBHOOK_SIGNING_SECRET", "unit-test-webhook-secret")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
