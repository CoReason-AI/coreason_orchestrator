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
from typing import Any

from coreason_manifest.spec.ontology import (
    EpistemicLedgerState,
    ObservationEvent,
    SystemFaultEvent,
)
from hypothesis import given, settings
from hypothesis.strategies import dictionaries, integers, lists, text

from coreason_orchestrator.hydration import compile_state_hydration


def _create_mock_hash(seed: str) -> str:
    """Helper to generate a valid SHA-256 string for valid CIDs."""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def test_compile_state_hydration_empty_ledger() -> None:
    """Verifies that an empty ledger correctly produces an empty list of CIDs."""
    ledger = EpistemicLedgerState(history=[])

    manifest = compile_state_hydration(
        ledger=ledger,
        coordinate="session-empty",
        context={"key": "val"},
        max_tokens=2048,
    )

    assert manifest.crystallized_ledger_cids == []
    assert manifest.epistemic_coordinate == "session-empty"
    assert manifest.working_context_variables == {"key": "val"}
    assert manifest.max_retained_tokens == 2048


def test_compile_state_hydration_populated_ledger() -> None:
    """Verifies that a populated ledger compiles to a correctly ordered list of CIDs."""
    hash1 = _create_mock_hash("evt1")
    hash2 = _create_mock_hash("evt2")

    ledger = EpistemicLedgerState(
        history=[
            SystemFaultEvent(event_id=hash1, timestamp=1.0, type="system_fault"),
            ObservationEvent(event_id=hash2, timestamp=2.0, type="observation", payload={"a": "b"}),
        ]
    )

    manifest = compile_state_hydration(
        ledger=ledger,
        coordinate="session-populated",
        context={},
        max_tokens=4096,
    )

    assert manifest.crystallized_ledger_cids == sorted([hash1, hash2])
    assert manifest.epistemic_coordinate == "session-populated"
    assert manifest.working_context_variables == {}
    assert manifest.max_retained_tokens == 4096


@given(  # type: ignore[misc]
    coordinate=text(min_size=1),
    context=dictionaries(text(min_size=1), text(min_size=1)),
    max_tokens=integers(min_value=1, max_value=128000),
    event_seeds=lists(text(min_size=1), max_size=10),
)
@settings(max_examples=10)  # type: ignore[misc]
def test_compile_state_hydration_hypothesis(
    coordinate: str, context: dict[str, Any], max_tokens: int, event_seeds: list[str]
) -> None:
    """Uses Hypothesis to verify valid output over a range of inputs."""
    # Synthesize deterministic mock events
    mock_events: list[Any] = []
    expected_cids: list[str] = []
    for i, seed in enumerate(event_seeds):
        # We need realistic valid CIDs (SHA-256 strings)
        valid_cid = _create_mock_hash(f"{i}-{seed}")
        expected_cids.append(valid_cid)
        mock_events.append(SystemFaultEvent(event_id=valid_cid, timestamp=float(i), type="system_fault"))

    ledger = EpistemicLedgerState(history=mock_events)

    manifest = compile_state_hydration(
        ledger=ledger,
        coordinate=coordinate,
        context=context,
        max_tokens=max_tokens,
    )

    assert manifest.epistemic_coordinate == coordinate
    assert manifest.working_context_variables == context
    assert manifest.max_retained_tokens == max_tokens
    # Note: StateHydrationManifest deterministically sorts array elements
    # during initialization in coreason_manifest to enforce reproducible hashes.
    assert manifest.crystallized_ledger_cids == sorted(expected_cids)
