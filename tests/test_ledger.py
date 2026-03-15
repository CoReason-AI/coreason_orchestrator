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
    DefeasibleCascadeEvent,
    EpistemicLedgerState,
    ObservationEvent,
    RollbackIntent,
    SystemFaultEvent,
)
from hypothesis import given, settings
from hypothesis.strategies import dictionaries, floats, lists, text

from coreason_orchestrator.ledger import append_event, apply_rollback
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


def test_apply_rollback_basic() -> None:
    """Verifies applying a rollback updates the arrays without mutating the original ledger."""
    ledger = EpistemicLedgerState(history=[])

    rollback = RollbackIntent(
        request_id="req1",
        target_event_id="target1",
        invalidated_node_ids=["node1", "node2"],
    )

    cascade = DefeasibleCascadeEvent(
        cascade_id="cascade1",
        root_falsified_event_id="target1",
        propagated_decay_factor=0.5,
        quarantined_event_ids=["evt1", "evt2"],
    )

    new_ledger = apply_rollback(ledger, rollback, cascade)

    # Verify original ledger is not mutated
    assert ledger.active_rollbacks == []
    assert ledger.active_cascades == []

    # Verify new ledger has the correct items appended
    assert new_ledger.active_rollbacks is not None
    assert len(new_ledger.active_rollbacks) == 1
    assert new_ledger.active_rollbacks[0].request_id == "req1"

    assert new_ledger.active_cascades is not None
    assert len(new_ledger.active_cascades) == 1
    assert new_ledger.active_cascades[0].cascade_id == "cascade1"

    # Verify deep copy was used (modifying the original intent should not affect the ledger)
    rollback.invalidated_node_ids.append("node3")
    assert "node3" not in new_ledger.active_rollbacks[0].invalidated_node_ids

    cascade.quarantined_event_ids.append("evt3")
    assert "evt3" not in new_ledger.active_cascades[0].quarantined_event_ids


def test_apply_rollback_existing_elements() -> None:
    """Verifies appending works when there are already existing rollbacks and cascades."""
    ledger = EpistemicLedgerState(
        history=[],
        active_rollbacks=[
            RollbackIntent(request_id="req_old", target_event_id="target_old", invalidated_node_ids=["node_old"])
        ],
        active_cascades=[
            DefeasibleCascadeEvent(
                cascade_id="cas_old",
                root_falsified_event_id="target_old",
                propagated_decay_factor=0.1,
                quarantined_event_ids=["evt_old"],
            )
        ],
    )

    rollback = RollbackIntent(
        request_id="req_new",
        target_event_id="target_new",
        invalidated_node_ids=["node_new"],
    )

    cascade = DefeasibleCascadeEvent(
        cascade_id="cas_new",
        root_falsified_event_id="target_new",
        propagated_decay_factor=0.9,
        quarantined_event_ids=["evt_new"],
    )

    new_ledger = apply_rollback(ledger, rollback, cascade)

    assert new_ledger.active_rollbacks is not None
    assert len(new_ledger.active_rollbacks) == 2
    assert new_ledger.active_rollbacks[1].request_id == "req_new"

    assert new_ledger.active_cascades is not None
    assert len(new_ledger.active_cascades) == 2
    assert new_ledger.active_cascades[1].cascade_id == "cas_new"

    # Ensure original ledger unmodified
    assert ledger.active_rollbacks is not None
    assert len(ledger.active_rollbacks) == 1


@given(  # type: ignore[misc]
    request_id=text(min_size=1),
    target_event_id=text(min_size=1),
    invalidated_node_ids=lists(text()),
    cascade_id=text(min_size=1),
    root_falsified_event_id=text(min_size=1),
    propagated_decay_factor=floats(min_value=0.0, max_value=1.0),
    quarantined_event_ids=lists(text(), min_size=1),
)
@settings(max_examples=10)  # type: ignore[misc]
def test_apply_rollback_hypothesis(
    request_id: str,
    target_event_id: str,
    invalidated_node_ids: list[str],
    cascade_id: str,
    root_falsified_event_id: str,
    propagated_decay_factor: float,
    quarantined_event_ids: list[str],
) -> None:
    """Uses hypothesis to verify apply_rollback works with varied structural configurations."""
    ledger = EpistemicLedgerState(history=[])

    rollback = RollbackIntent(
        request_id=request_id,
        target_event_id=target_event_id,
        invalidated_node_ids=invalidated_node_ids,
    )

    cascade = DefeasibleCascadeEvent(
        cascade_id=cascade_id,
        root_falsified_event_id=root_falsified_event_id,
        propagated_decay_factor=propagated_decay_factor,
        quarantined_event_ids=quarantined_event_ids,
    )

    new_ledger = apply_rollback(ledger, rollback, cascade)

    assert new_ledger.active_rollbacks is not None
    assert len(new_ledger.active_rollbacks) == 1
    assert new_ledger.active_rollbacks[0].request_id == request_id

    assert new_ledger.active_cascades is not None
    assert len(new_ledger.active_cascades) == 1
    assert new_ledger.active_cascades[0].cascade_id == cascade_id

    # Original remains untouched
    assert ledger.active_rollbacks == []
    assert ledger.active_cascades == []
