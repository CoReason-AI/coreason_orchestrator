# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import asyncio
import sys
import urllib.request
from typing import Any

from loguru import logger

__all__ = ["logger"]

# Remove default handler
logger.remove()

# Sink 1: Stdout (Human-readable)
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)


def _serialize_to_log_event(message: Any) -> str:
    import time

    from coreason_manifest.spec.ontology import LogEvent

    record = message.record

    # Map standard log levels to ontological severity
    severity_map = {"DEBUG": "DEBUG", "INFO": "INFO", "WARNING": "WARNING", "ERROR": "ERROR", "CRITICAL": "CRITICAL"}

    level = severity_map.get(record["level"].name, "INFO")

    event = LogEvent.model_construct(
        timestamp=time.time(),
        level=level,  # type: ignore[arg-type]
        message=record["message"],
        context_profile=record["extra"],
    )

    return str(event.model_dump_json() + "\n")


class AsyncTelemetrySink:
    """A Cloud-Native telemetry shipper for JSON-serialized LogEvent strings."""

    def __init__(self, endpoint: str = "http://localhost:4318/v1/logs", batch_size: int = 100):
        self.endpoint = endpoint
        self.batch_size = batch_size
        self._batch: list[str] = []
        self._flush_task: asyncio.Task[None] | None = None

    def _flush_sync(self, payload: str) -> None:
        """Synchronously posts the batched log messages to the configured HTTP endpoint."""
        try:
            req = urllib.request.Request(  # noqa: S310
                self.endpoint,
                data=payload.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):  # noqa: S310
                pass
        except Exception as e:
            # Fallback for telemetry failures, printed to stderr
            print(f"Telemetry export failed: {e}", file=sys.stderr)

    async def _flush_current_batch(self) -> None:
        """Flushes the current accumulated batch of log messages."""
        if not self._batch:
            return
        # Join batch into a newline-delimited JSON payload
        payload = "".join(self._batch)
        self._batch.clear()
        await asyncio.to_thread(self._flush_sync, payload)

    async def _flush_loop(self) -> None:
        """A background timer that periodically flushes stale logs."""
        try:
            while True:
                await asyncio.sleep(1.0)
                if self._batch:
                    await self._flush_current_batch()
        except asyncio.CancelledError:
            pass

    async def write(self, message: str) -> None:
        """Asynchronously queues the log message and flushes when batch is full."""
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

        self._batch.append(message)
        if len(self._batch) >= self.batch_size:
            await self._flush_current_batch()


# Sink 2: Cloud-Native Telemetry (JSON, Async, Ontological Telemetry Sink)
# We need to make sure loguru has a running event loop to hook into,
# or we just let it execute dynamically by removing enqueue=True for the coroutine,
# but it's better to provide an actual event loop or disable enqueue for async sinks.
# loguru automatically handles enqueuing if it's an async function anyway!
logger.add(
    AsyncTelemetrySink().write,
    format=_serialize_to_log_event,
    enqueue=False,  # Loguru manages async tasks automatically when the sink is a coroutine
    level="INFO",
)
