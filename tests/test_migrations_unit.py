from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Index

from app.models import Base, Outbox, Payment, PaymentEventLog, WebhookDelivery


def test_migrations_have_a_single_ordered_head():
    root = Path(__file__).parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    scripts = ScriptDirectory.from_config(config)
    assert scripts.get_heads() == ["0003"]
    assert scripts.get_revision("0003").down_revision == "0002"
    assert scripts.get_revision("0002").down_revision == "0001"
    assert scripts.get_revision("0001").down_revision is None


def test_model_schema_has_expected_tables_and_constraints():
    assert {"payments", "outbox", "payment_events", "webhook_deliveries"} <= set(Base.metadata.tables)
    assert Payment.__table__.c.idempotency_key.unique
    assert Outbox.__table__.c.lock_token.nullable
    assert Payment.__table__.c.processing_token.nullable
    assert Payment.__table__.c.webhook_token.nullable
    assert any(isinstance(item, Index) for item in PaymentEventLog.__table__.indexes)
    assert "attempt" in WebhookDelivery.__table__.c
