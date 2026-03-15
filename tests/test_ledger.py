# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_orchestrator

from typing import Any

from coreason_manifest.spec.ontology import (
    EpistemicLedgerState,
    ObservationEvent,
    SystemFaultEvent,
)
from hypothesis import given, settings
from hypothesis.strategies import dictionaries, text

from coreason_orchestrator.ledger import append_event
from coreason_orchestrator.utils.crypto import calculate_event_hash


def test_append_event_system_fault() -> None:
    """Verifies that appending a SystemFaultEvent computes and assigns the event_id."""
    initial_ledger = EpistemicLedgerState(history=[])

    event = SystemFaultEvent(
        event_id="placeholder",
        timestamp=123456789.0,
        type="system_fault",
    )

    new_ledger = append_event(initial_ledger, event)

    # Verify original objects were not mutated
    assert event.event_id == "placeholder"
    assert len(initial_ledger.history) == 0

    # Verify new ledger has the event
    assert len(new_ledger.history) == 1
    new_event = new_ledger.history[0]

    # Verify type and event_id assignment
    assert isinstance(new_event, SystemFaultEvent)
    assert new_event.event_id != "placeholder"

    # Verify the hash matches expectation for temp event
    temp_event = event.model_copy(update={"event_id": ""})
    expected_hash = calculate_event_hash(temp_event)
    assert new_event.event_id == expected_hash


def test_append_event_multiple() -> None:
    """Verifies that multiple events can be appended."""
    ledger = EpistemicLedgerState(history=[])

    event1 = SystemFaultEvent(event_id="e1", timestamp=1.0, type="system_fault")
    ledger1 = append_event(ledger, event1)

    event2 = SystemFaultEvent(event_id="e2", timestamp=2.0, type="system_fault")
    ledger2 = append_event(ledger1, event2)

    assert len(ledger2.history) == 2
    assert ledger2.history[0].timestamp == 1.0
    assert ledger2.history[1].timestamp == 2.0

    assert ledger2.history[0].event_id != "e1"
    assert ledger2.history[1].event_id != "e2"


def test_append_event_observation() -> None:
    """Verifies appending an ObservationEvent works correctly."""
    ledger = EpistemicLedgerState(history=[])

    event = ObservationEvent(event_id="o1", timestamp=3.0, type="observation", payload={"key": "value"})

    new_ledger = append_event(ledger, event)
    assert len(new_ledger.history) == 1
    assert isinstance(new_ledger.history[0], ObservationEvent)
    assert new_ledger.history[0].payload == {"key": "value"}


@given(payload=dictionaries(text(min_size=1), text(min_size=1)))  # type: ignore[misc]
@settings(max_examples=10)  # type: ignore[misc]
def test_append_event_hypothesis(payload: dict[str, Any]) -> None:
    """Uses hypothesis to verify append event works on varied payloads."""
    ledger = EpistemicLedgerState(history=[])

    event = ObservationEvent(event_id="placeholder", timestamp=4.0, type="observation", payload=payload)

    new_ledger = append_event(ledger, event)

    assert len(new_ledger.history) == 1
    new_event = new_ledger.history[0]
    assert isinstance(new_event, ObservationEvent)
    assert new_event.event_id != "placeholder"
    assert new_event.event_id == calculate_event_hash(event.model_copy(update={"event_id": ""}))
