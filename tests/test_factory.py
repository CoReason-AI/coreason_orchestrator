# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from coreason_orchestrator.factory import EventFactory
from coreason_orchestrator.utils.crypto import calculate_event_hash


class MockSimpleEvent(BaseModel):
    event_id: str
    data: str
    count: int


class MockComplexEvent(BaseModel):
    event_id: str
    nested: dict[str, str | int]
    flags: list[bool]


class MockNoEventIdEvent(BaseModel):
    data: str


class MockDefaultCoercionEvent(BaseModel):
    event_id: str
    count: int
    status: str = "pending"


def test_event_factory_no_event_id() -> None:
    """Test that EventFactory generates event correctly even if event_id is missing."""
    event = EventFactory.build_event(MockNoEventIdEvent, data="test_data")
    assert not hasattr(event, "event_id")
    assert event.data == "test_data"


def test_event_factory_with_pre_existing_event_id() -> None:
    """Test that EventFactory generates event correctly even if event_id is pre-existing."""
    event = EventFactory.build_event(MockSimpleEvent, event_id="fake_id", data="test_data", count=42)

    temp_event = MockSimpleEvent(event_id="", data="test_data", count=42)
    expected_hash = calculate_event_hash(temp_event.model_dump(exclude={"event_id"}))
    assert event.event_id == expected_hash


def test_event_factory_creates_event_with_hash() -> None:
    """Test that EventFactory generates a mathematically bound RFC 8785 hash and assigns it."""
    # Build event using factory
    event = EventFactory.build_event(MockSimpleEvent, data="test_data", count=42)

    # Reconstruct what the temporary payload should have been
    temp_event = MockSimpleEvent(event_id="", data="test_data", count=42)
    expected_hash = calculate_event_hash(temp_event.model_dump(exclude={"event_id"}))

    assert event.event_id == expected_hash
    assert event.data == "test_data"
    assert event.count == 42


def test_event_factory_with_defaults_and_coercion() -> None:
    """Test that EventFactory correctly incorporates Pydantic's default values and type coercion into the hash."""
    # Pass string "42" instead of int 42, and omit the status field to trigger the default "pending"
    event = EventFactory.build_event(MockDefaultCoercionEvent, count="42")  # type: ignore[arg-type]

    # The expected hash MUST be computed from the coerced and defaulted values
    temp_event = MockDefaultCoercionEvent(event_id="", count=42, status="pending")
    expected_hash = calculate_event_hash(temp_event.model_dump(exclude={"event_id"}))

    assert event.event_id == expected_hash
    assert event.count == 42
    assert event.status == "pending"


@given(  # type: ignore[misc]
    data=st.text(), count=st.integers()
)
@settings(  # type: ignore[misc]
    max_examples=50
)
def test_event_factory_deterministic_hashing(data: str, count: int) -> None:
    """Test that EventFactory produces consistent and deterministic hashes for identical inputs."""
    event1 = EventFactory.build_event(MockSimpleEvent, data=data, count=count)
    event2 = EventFactory.build_event(MockSimpleEvent, data=data, count=count)

    assert event1.event_id == event2.event_id
    assert event1.model_dump() == event2.model_dump()


@given(  # type: ignore[misc]
    nested=st.dictionaries(keys=st.text(), values=st.text() | st.integers()), flags=st.lists(st.booleans())
)
@settings(  # type: ignore[misc]
    max_examples=50
)
def test_event_factory_with_complex_types(nested: dict[str, str | int], flags: list[bool]) -> None:
    """Test that EventFactory handles complex nested payloads."""
    event = EventFactory.build_event(MockComplexEvent, nested=nested, flags=flags)

    # Reconstruct what the temporary payload should have been
    temp_event = MockComplexEvent(event_id="", nested=nested, flags=flags)
    expected_hash = calculate_event_hash(temp_event.model_dump(exclude={"event_id"}))

    assert event.event_id == expected_hash
    assert event.nested == nested
    assert event.flags == flags
