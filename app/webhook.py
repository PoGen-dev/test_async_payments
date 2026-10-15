import asyncio
import hashlib
import hmac
import ipaddress
import json
import socket
import time
from urllib.parse import urlsplit

import httpcore
import httpx

from app.config import get_settings


def _parse_target(url: str):
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        raise ValueError("Invalid webhook URL")
    return parts


async def _resolve(host: str, port: int, allow_private: bool) -> list[str]:
    addresses = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    ips = sorted({item[4][0] for item in addresses})
    if not ips or (not allow_private and any(not ipaddress.ip_address(ip).is_global for ip in ips)):
        raise ValueError("Webhook target resolves to a non-public address")
    return ips


async def validate_webhook_target(url: str, allow_private: bool) -> None:
    parts = _parse_target(url)
    await _resolve(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80), allow_private)


class ValidatedNetworkBackend(httpcore.AsyncNetworkBackend):
    """Pins vetted DNS results to the actual TCP destination, preserving TLS SNI."""

    def __init__(self, allow_private: bool):
        self.allow_private = allow_private
        self.backend = httpcore.AnyIOBackend()

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        if isinstance(host, bytes):
            host = host.decode("ascii")
        addresses = await _resolve(host, port, self.allow_private)
        return await self.backend.connect_tcp(
            addresses[0], port, timeout=timeout, local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):
        raise ValueError("Unix sockets are not valid webhook destinations")


class ValidatedTransport(httpx.AsyncHTTPTransport):
    def __init__(self, allow_private: bool):
        super().__init__(retries=0, trust_env=False)
        # httpx delegates request/response handling to its HTTPTransport implementation.
        self._pool = httpcore.AsyncConnectionPool(network_backend=ValidatedNetworkBackend(allow_private))


def signature(secret: str, timestamp: str, event_id: str, body: bytes) -> str:
    data = timestamp.encode() + b"." + event_id.encode() + b"." + body
    return "v1=" + hmac.new(secret.encode(), data, hashlib.sha256).hexdigest()


class WebhookSender:
    def __init__(self, allow_private: bool = False, signing_secret: str | None = None, timeout: float = 5.0):
        self.allow_private = allow_private
        self.signing_secret = signing_secret if signing_secret is not None else get_settings().webhook_signing_secret
        self.timeout = timeout
        if not self.signing_secret:
            raise ValueError("WEBHOOK_SIGNING_SECRET must be configured")

    async def send(self, url: str, payload: dict) -> int:
        parts = _parse_target(url)
        if not self.allow_private and parts.scheme != "https":
            raise ValueError("Public webhooks must use HTTPS")
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        timestamp = str(int(time.time()))
        event_id = str(payload["event_id"])
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": event_id,
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": signature(self.signing_secret, timestamp, event_id, body),
        }
        async with httpx.AsyncClient(
            transport=ValidatedTransport(self.allow_private), timeout=self.timeout,
            follow_redirects=False, trust_env=False,
        ) as client:
            response = await client.post(url, content=body, headers=headers)
            if not 200 <= response.status_code < 300:
                raise httpx.HTTPStatusError(
                    f"Webhook returned {response.status_code}", request=response.request, response=response,
                )
            return response.status_code
