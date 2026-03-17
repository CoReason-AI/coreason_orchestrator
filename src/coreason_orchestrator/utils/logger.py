# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import sys
from pathlib import Path
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

# Ensure logs directory exists
log_path = Path("logs")
if not log_path.exists():
    log_path.mkdir(parents=True, exist_ok=True)


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


# Sink 2: File (JSON, Rotation, Retention, Ontological Telemetry Sink)
logger.add(
    "logs/app.log",
    rotation="500 MB",
    retention="10 days",
    format=_serialize_to_log_event,
    enqueue=True,
    level="INFO",
)
