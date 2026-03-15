# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

import hashlib
import json
from typing import Any

from pydantic import BaseModel


def calculate_event_hash(payload: dict[str, Any] | BaseModel) -> str:
    """
    Calculates the JSON Control Hash (RFC 8785 approximation) for a given payload.

    Args:
        payload: A Pydantic model or a dictionary representing the event payload.

    Returns:
        A deterministic SHA-256 hash of the canonicalized JSON representation.
    """
    # dump with sorted keys to ensure determinism
    # using mode="json" ensures all types are converted to JSON-serializable primitives
    data = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload

    # Canonicalize the JSON: no spaces, sorted keys
    canonical_json = json.dumps(data, separators=(",", ":"), sort_keys=True, ensure_ascii=False)

    # Calculate SHA-256 hash of the UTF-8 encoded canonical JSON
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
