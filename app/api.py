import hmac
import logging
import os
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, start_http_server
from sqlalchemy import select, text

from app.config import get_settings
from app.db import make_engine, make_session_factory
from app.models import PaymentEventLog, WebhookDelivery
from app.observability import PAYMENT_CREATED, configure_logging
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail, PaymentEventDetail, WebhookDeliveryDetail
from app.service import IdempotencyConflict, create_payment, get_payment

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(value: str | None = Depends(api_key_header)) -> None:
    if value is None or not hmac.compare_digest(value, get_settings().api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    start_http_server(get_settings().api_metrics_port, addr="0.0.0.0")
    engine = make_engine()
    app.state.session_factory = make_session_factory(engine)
    try:
        yield
    finally:
        await engine.dispose()


docs_enabled = os.environ.get("ENABLE_DOCS", "false").lower() == "true"
app = FastAPI(
    lifespan=lifespan,
    docs_url="/docs" if docs_enabled else None,
    redoc_url=None,
    openapi_url="/openapi.json" if docs_enabled else None,
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", "")
    if len(request_id) > 80 or not request_id.isascii() or not request_id.isprintable():
        request_id = ""
    request_id = request_id or str(uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    logger.info("HTTP %s %s %s", request.method, request.url.path, response.status_code,
                extra={"correlation_id": request_id})
    return response


@app.get("/health/live", dependencies=[Depends(verify_api_key)], include_in_schema=False)
async def live():
    return {"status": "ok"}


@app.get("/health/ready", dependencies=[Depends(verify_api_key)], include_in_schema=False)
async def ready(request: Request):
    try:
        async with request.app.state.session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(503, detail="Database unavailable") from None
    return {"status": "ok"}


@app.get("/metrics", dependencies=[Depends(verify_api_key)], include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post(
    "/api/v1/payments", status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted, dependencies=[Depends(verify_api_key)],
)
async def post_payment(
    body: PaymentCreate,
    request: Request,
    idempotency_key: str = Header(min_length=1, max_length=255, alias="Idempotency-Key"),
):
    if not get_settings().webhook_allow_private and body.webhook_url.scheme != "https":
        raise HTTPException(status_code=422, detail="Public webhook URLs must use HTTPS")
    try:
        payment = await create_payment(request.app.state.session_factory, body, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(status_code=409, detail="Idempotency key reused with different payload") from None
    PAYMENT_CREATED.inc()
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@app.get(
    "/api/v1/payments/{payment_id}", response_model=PaymentDetail,
    dependencies=[Depends(verify_api_key)],
)
async def read_payment(payment_id: UUID, request: Request):
    payment = await get_payment(request.app.state.session_factory, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetail(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at,
        amount=payment.amount, currency=payment.currency, description=payment.description,
        metadata=payment.metadata_json, webhook_url=payment.webhook_url,
        processed_at=payment.processed_at, webhook_delivered_at=payment.webhook_delivered_at,
    )


@app.get(
    "/api/v1/payments/{payment_id}/webhook-deliveries",
    response_model=list[WebhookDeliveryDetail],
    dependencies=[Depends(verify_api_key)],
)
async def read_webhook_deliveries(payment_id: UUID, request: Request):
    payment = await get_payment(request.app.state.session_factory, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    async with request.app.state.session_factory() as session:
        rows = (await session.scalars(
            select(WebhookDelivery).where(WebhookDelivery.payment_id == payment_id).order_by(WebhookDelivery.attempt)
        )).all()
    return [WebhookDeliveryDetail.model_validate(row) for row in rows]


@app.get(
    "/api/v1/payments/{payment_id}/events",
    response_model=list[PaymentEventDetail],
    dependencies=[Depends(verify_api_key)],
)
async def read_payment_events(payment_id: UUID, request: Request):
    payment = await get_payment(request.app.state.session_factory, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    async with request.app.state.session_factory() as session:
        rows = (await session.scalars(
            select(PaymentEventLog).where(PaymentEventLog.payment_id == payment_id)
            .order_by(PaymentEventLog.created_at, PaymentEventLog.id)
        )).all()
    return [PaymentEventDetail.model_validate(row) for row in rows]
