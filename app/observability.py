import json
import logging
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram

PAYMENT_CREATED = Counter("payments_created_total", "Accepted payment requests")
PAYMENT_PROCESSED = Counter("payments_processed_total", "Terminal payment outcomes", ["status"])
WEBHOOK_ATTEMPTS = Counter("webhook_attempts_total", "Webhook outcomes", ["result"])
OUTBOX_PUBLISH = Counter("outbox_publish_total", "Outbox publishing outcomes", ["result"])
PROCESSING_TIME = Histogram("payment_processing_seconds", "Payment message processing time")
OUTBOX_PENDING = Gauge("outbox_pending", "Pending outbox events")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ("payment_id", "event_id", "correlation_id"):
            value = getattr(record, field, None)
            if value is not None:
                data[field] = str(value)
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
