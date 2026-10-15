import asyncio
import hashlib
import random


class SimulatedGateway:
    def __init__(self, min_delay: float = 2, max_delay: float = 5):
        if min_delay < 0 or max_delay < min_delay:
            raise ValueError("invalid gateway delay range")
        self.min_delay = min_delay
        self.max_delay = max_delay

    async def charge(self, idempotency_key: str) -> bool:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        # Deterministic simulated outcome for the same external charge key.
        outcome = int.from_bytes(hashlib.sha256(idempotency_key.encode()).digest()[:8], "big")
        return outcome / (1 << 64) < 0.9
