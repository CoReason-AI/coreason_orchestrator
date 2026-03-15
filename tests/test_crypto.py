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

from hypothesis import given, settings
from hypothesis.strategies import dictionaries, floats, integers, none, one_of, recursive, text
from pydantic import BaseModel, Field

from coreason_orchestrator.utils.crypto import calculate_event_hash


class DummyNestedModel(BaseModel):
    id: str
    value: int


class DummyEvent(BaseModel):
    event_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    nested: DummyNestedModel | None = None


def test_calculate_event_hash_deterministic_dict() -> None:
    """Tests that calculate_event_hash is deterministic for dictionaries regardless of key order."""
    dict1 = {"a": 1, "b": 2, "c": 3}
    dict2 = {"c": 3, "a": 1, "b": 2}

    hash1 = calculate_event_hash(dict1)
    hash2 = calculate_event_hash(dict2)

    assert hash1 == hash2

    # Manually verify against expected json dump behavior
    expected_json = json.dumps({"a": 1, "b": 2, "c": 3}, separators=(",", ":"), sort_keys=True)
    expected_hash = hashlib.sha256(expected_json.encode("utf-8")).hexdigest()
    assert hash1 == expected_hash


def test_calculate_event_hash_pydantic_model() -> None:
    """Tests that calculate_event_hash handles Pydantic models deterministically."""
    model = DummyEvent(data={"c": 3, "a": 1}, nested=DummyNestedModel(id="test", value=42))

    hash1 = calculate_event_hash(model)

    # Compare with dictionary representation
    model_dict = {"event_id": None, "data": {"a": 1, "c": 3}, "nested": {"id": "test", "value": 42}}
    hash2 = calculate_event_hash(model_dict)

    assert hash1 == hash2


def test_calculate_event_hash_ensure_ascii_false() -> None:
    """Tests that Unicode characters are handled correctly without ascii escaping."""
    dict_with_unicode = {"name": "Jules 🐶"}
    hash1 = calculate_event_hash(dict_with_unicode)

    # Ensure it's not escaping unicode sequences like \\U
    canonical_json = json.dumps(dict_with_unicode, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    expected_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    assert hash1 == expected_hash


# Use hypothesis to test various JSON-serializable payloads for basic properties
json_strategy = recursive(
    one_of(none(), integers(), floats(allow_nan=False, allow_infinity=False), text()),
    lambda children: one_of(
        dictionaries(text(), children),
    ),
    max_leaves=5,
)


@given(payload=dictionaries(text(min_size=1), text(min_size=1)))  # type: ignore[misc]
@settings(max_examples=10)  # type: ignore[misc]
def test_calculate_event_hash_hypothesis(payload: dict[str, Any]) -> None:
    """Tests that calculate_event_hash works reliably on arbitrary json-like structures."""
    result_hash = calculate_event_hash(payload)
    assert isinstance(result_hash, str)
    assert len(result_hash) == 64  # SHA-256 hex digest length
